import postgres from 'postgres';
import * as zod from 'zod';

export class Collection<T extends zod.core.$ZodLooseShape> {
  public 'Type': zod.infer<typeof this.zod>;

  public name: string;

  public schema: {[name: string]: {
    type: string;
    length: number;
    default: string;
    nullable: boolean;
  }} = {};

  protected zod: zod.ZodObject<T>;
  protected sql!: ReturnType<typeof postgres>;

  constructor(name: string, zodObject: zod.ZodObject<T>, sql: ReturnType<typeof postgres>) {
    this.name = name;
    this.zod = zodObject;
    this.sql = sql;
  }

  public async create(input: zod.input<typeof this.zod>): Promise<this['Type']> {
    const data = await this.zod.parseAsync(input);

    const sql = this.sql;
    const result = await sql`INSERT INTO ${ sql.unsafe(this.name) } ${ sql(data as any, Object.keys(data)) }`;

    console.log("RESULT:", result);

    // return result[0] as this['Type'];
    return data;
  }

  public async migrate() {
    // Check if table exists
    const tableExists = await this.sql`
      SELECT EXISTS (
        SELECT FROM information_schema.tables
        WHERE table_name = ${this.name}
      )
    `;

    const zodSchema = this.zodToTableSchema();

    if (!tableExists[0]?.exists) {
      // Create table if it doesn't exist
      await this.createTable(zodSchema);

    } else {
      // Get existing columns
      const existingColumns = await this.sql`
        select
          column_name, data_type, character_maximum_length, column_default, is_nullable
        from
          information_schema.columns
        where
          table_name = ${this.name}
      `;

      // Compare and alter table if needed
      await this.alterTable(existingColumns, zodSchema);
    }
  }

  private zodToTableSchema() {
    const schema: {[key: string]: {type: string, nullable: boolean, default?: any}} = {};

    // Convert the entire Zod schema to JSON Schema once
    const jsonSchema = zod.toJSONSchema(this.zod);

    // Work with the JSON Schema properties
    if (jsonSchema.properties) {
      const requiredFields = new Set(jsonSchema.required || []);

      for (const [key, propertySchema] of Object.entries(jsonSchema.properties)) {
        const isRequired = requiredFields.has(key);
        const zodProperty = this.zod.shape[key] as zod.ZodType;

        schema[key] = this.jsonSchemaToPostgresType(
          propertySchema as any,
          !isRequired,
          zodProperty
        );
      }
    }

    return schema;
  }

  private jsonSchemaToPostgresType(
    jsonSchema: any,
    isNullable: boolean,
    zodProperty: zod.ZodType
  ): {type: string, nullable: boolean, default?: any} {
    let nullable = isNullable;
    let defaultValue: any = undefined;
    let workingSchema = jsonSchema;

    // Check if it's nullable (represented as oneOf with null)
    if (jsonSchema.oneOf && Array.isArray(jsonSchema.oneOf)) {
      const hasNull = jsonSchema.oneOf.some((schema: any) => schema.type === 'null');
      if (hasNull) {
        nullable = true;
        // Get the non-null schema
        const nonNullSchema = jsonSchema.oneOf.find((schema: any) => schema.type !== 'null');
        if (nonNullSchema) {
          workingSchema = nonNullSchema;
        }
      }
    }

    // Extract default value from the original Zod schema if it has ZodDefault
    if (zodProperty instanceof zod.ZodDefault) {
      defaultValue = typeof zodProperty.def.defaultValue === 'function'
        ? zodProperty.def.defaultValue()
        : zodProperty.def.defaultValue;
    }

    // Map JSON Schema types to PostgreSQL types
    switch (workingSchema.type) {
      case 'string':
        // Check for maxLength constraint
        if (workingSchema.maxLength && typeof workingSchema.maxLength === 'number') {
          return { type: `VARCHAR(${workingSchema.maxLength})`, nullable, default: defaultValue };
        }

        // Handle dates (they might come as strings with format)
        if (workingSchema.format === 'date-time' || workingSchema.format === 'date') {
          return { type: 'TIMESTAMP', nullable, default: defaultValue };
        }

        return { type: 'TEXT', nullable, default: defaultValue };

      case 'integer':
        return { type: 'INTEGER', nullable, default: defaultValue };

      case 'number':
        return { type: 'DECIMAL', nullable, default: defaultValue };

      case 'boolean':
        return { type: 'BOOLEAN', nullable, default: defaultValue };

      case 'array':
        return { type: 'JSONB', nullable, default: defaultValue };

      case 'object':
        return { type: 'JSONB', nullable, default: defaultValue };

      default:
        // Default fallback
        return { type: 'TEXT', nullable, default: defaultValue };
    }
  }

  private async createTable(schema: {[key: string]: {type: string, nullable: boolean, default?: any}}) {
    const columns = Object.entries(schema).map(([name, def]) => {
      let columnDef = `${name} ${def.type}`;

      if (!def.nullable) {
        columnDef += ' NOT NULL';
      }

      if (def.default !== undefined) {
        if (typeof def.default === 'string') {
          columnDef += ` DEFAULT '${def.default}'`;
        } else {
          columnDef += ` DEFAULT ${def.default}`;
        }
      }

      return columnDef;
    }).join(', ');

    const createTableSQL = `CREATE TABLE ${this.name} (${columns})`;
    console.log('Creating table:', createTableSQL);

    await this.sql.unsafe(createTableSQL);

    // Populate internal schema
    this.populateSchemaFromZod(schema);
  }

  private async alterTable(existingColumns: any[], newSchema: {[key: string]: {type: string, nullable: boolean, default?: any}}) {
    const existingColumnMap = new Map();
    existingColumns.forEach(col => {
      existingColumnMap.set(col.column_name, {
        type: col.data_type,
        nullable: col.is_nullable === 'YES',
        default: col.column_default
      });
    });

    // Add new columns
    for (const [columnName, columnDef] of Object.entries(newSchema)) {
      if (!existingColumnMap.has(columnName)) {
        let addColumnSQL = `ALTER TABLE ${this.name} ADD COLUMN ${columnName} ${columnDef.type}`;

        if (!columnDef.nullable) {
          addColumnSQL += ' NOT NULL';
        }

        if (columnDef.default !== undefined) {
          if (typeof columnDef.default === 'string') {
            addColumnSQL += ` DEFAULT '${columnDef.default}'`;
          } else {
            addColumnSQL += ` DEFAULT ${columnDef.default}`;
          }
        }

        console.log('Adding column:', addColumnSQL);
        await this.sql.unsafe(addColumnSQL);
      }
    }

    // Populate internal schema with existing columns and new schema
    this.populateSchemaFromExisting(existingColumns);
    this.populateSchemaFromZod(newSchema);

    // Note: We're not dropping columns that exist in DB but not in schema
    // This is a safety measure to prevent data loss
    // If you want to drop columns, you'd need to implement that logic here
  }

  private populateSchemaFromZod(schema: {[key: string]: {type: string, nullable: boolean, default?: any}}) {
    for (const [columnName, columnDef] of Object.entries(schema)) {
      const length = this.extractLengthFromType(columnDef.type);
      this.schema[columnName] = {
        type: columnDef.type,
        length: length,
        default: columnDef.default,
        nullable: columnDef.nullable,
      };
    }
  }

  private populateSchemaFromExisting(existingColumns: any[]) {
    existingColumns.forEach((column) => {
      this.schema[column.column_name] = {
        type: column.data_type,
        length: column.character_maximum_length || 0,
        default: column.column_default,
        nullable: column.is_nullable === 'YES',
      };
    });
  }

  private extractLengthFromType(type: string): number {
    const match = type.match(/\((\d+)\)/);
    return match && match[1] ? parseInt(match[1], 10) : 0;
  }

}

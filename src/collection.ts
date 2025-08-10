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

  public async create(input: zod.input<typeof this.zod>): Promise<this['Type']>;
  public async create(inputs: zod.input<typeof this.zod>[]): Promise<this['Type'][]>;
  public async create(inputOrInputs: zod.input<typeof this.zod> | zod.input<typeof this.zod>[]): Promise<this['Type'] | this['Type'][]> {
    const sql = this.sql;

    // handle array of inputs
    if (Array.isArray(inputOrInputs)) {
      const dataArray: any[] = [];

      // validate each input
      for (const input of inputOrInputs) {
        const data = await this.zod.parse(input);
        dataArray.push(data);
      }

      if (dataArray.length === 0) {
        return [];
      }

      let results: any;
      try {
        results = await sql`INSERT INTO ${sql.unsafe(this.name)} ${sql(dataArray)} RETURNING *`;
      } catch (e) {
        console.error(e);
        throw e;
      }

      return results.map((result: any, index: number) => ({ id: result.id, ...dataArray[index] }));

    } else {
      // handle single input
      const data = await this.zod.parse(inputOrInputs);

      let result: any;
      try {
        result = await sql`INSERT INTO ${sql.unsafe(this.name)} ${sql(data as any, Object.keys(data))} RETURNING *`;
      } catch (e) {
        console.error(e);
        throw e;
      }

      return { id: result[0].id, ...data };
    }
  }

  public async select(
    strings: TemplateStringsArray = Object.assign(["*"], { raw: ["*"] }),
    ...values: any[]
  ): Promise<this['Type'][]> {
    // SQL keywords that should come after the FROM clause
    const afterFromKeywords = /\b(WHERE|ORDER\s+BY|GROUP\s+BY|HAVING|LIMIT|OFFSET)\b/i;

    const firstString = strings[0] ?? "*";
    const firstRawString = strings.raw[0] ?? "*";

    // Check if the first string contains keywords that should come after FROM
    const match = firstString.match(afterFromKeywords);

    let modifiedFirstString: string;
    let modifiedFirstRawString: string;

    if (match) {
      // Insert FROM before the first keyword that should come after FROM
      const keywordIndex = match.index!;
      const beforeKeyword = firstString.substring(0, keywordIndex).trim();
      const fromKeyword = firstString.substring(keywordIndex);
      modifiedFirstString = `${beforeKeyword} FROM ${this.name} ${fromKeyword}`;

      const rawKeywordIndex = firstRawString.search(afterFromKeywords);
      const beforeKeywordRaw = firstRawString.substring(0, rawKeywordIndex).trim();
      const fromKeywordRaw = firstRawString.substring(rawKeywordIndex);
      modifiedFirstRawString = `${beforeKeywordRaw} FROM ${this.name} ${fromKeywordRaw}`;
    } else {
      // No keywords found, add FROM at the end as before
      modifiedFirstString = `${firstString} FROM ${this.name}`;
      modifiedFirstRawString = `${firstRawString} FROM ${this.name}`;
    }

    const newStrings = Object.assign(
      [
        `SELECT ${modifiedFirstString}`,
        ...strings.slice(1)
      ], {
        raw: [
          `SELECT ${modifiedFirstRawString}`,
          ...strings.raw.slice(1)
        ]
      }
    ) as TemplateStringsArray;

    const result = await this.sql(newStrings, ...values);

    return result.map((row: any, _: number) => {
      // Convert null values to undefined and handle type conversions
      const cleanedRow = Object.fromEntries(
        Object.entries(row).map(([key, value]) => {
          if (value === null) {
            return [key, undefined];
          }

          // Get the Zod type for this field to determine if we need type conversion
          const zodProperty = this.zod.shape[key] as zod.ZodType;

          // Handle ZodOptional by getting the inner type
          let innerType = zodProperty;
          if (zodProperty instanceof zod.ZodOptional) {
            innerType = zodProperty.unwrap();
          }

          // Convert string numbers back to actual numbers for numeric fields
          if (innerType instanceof zod.ZodNumber && typeof value === 'string') {
            const numValue = Number(value);
            return [key, isNaN(numValue) ? value : numValue];
          }

          return [key, value];
        })
      );
      return this.zod.parse(cleanedRow);
    }) as this['Type'][];
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

  public async drop() {
    await this.sql`DROP TABLE IF EXISTS ${this.sql.unsafe(this.name)}`;
  }

  private zodToTableSchema() {
    const schema: {[key: string]: {type: string, nullable: boolean, default?: any}} = {};

    // Convert the entire Zod schema to JSON Schema once
    const jsonSchema = zod.toJSONSchema(this.zod);

    // Work with the JSON Schema properties
    if (jsonSchema.properties) {
      const requiredFields = new Set(jsonSchema.required || []);

      for (const [field, propertySchema] of Object.entries(jsonSchema.properties)) {
        const isRequired = requiredFields.has(field);
        const zodProperty = this.zod.shape[field] as zod.ZodType;

        schema[field] = this.jsonSchemaToPostgresType(
          field,
          propertySchema,
          !isRequired,
          zodProperty
        );
      }
    }

    return schema;
  }

  private jsonSchemaToPostgresType(
    field: string,
    jsonSchema: any,
    isNullable: boolean,
    zodProperty: zod.ZodType
  ): {type: string, nullable: boolean, default?: any} {
    let nullable = isNullable;
    let defaultValue: any = undefined;
    let workingSchema = jsonSchema;

    console.log("JSON SCHEMA:", jsonSchema);

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

  private buildColumnDefinition(name: string, def: {type: string, nullable: boolean, default?: any}): string {
    let columnDef = name;

    if (name === 'id') {
      columnDef += ` INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY`;

    } else {
      columnDef += ` ${def.type}`;

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

    }

    return columnDef;
  }

  private async createTable(schema: {[key: string]: {type: string, nullable: boolean, default?: any}}) {
    const columns = Object.entries(schema).map(([name, def]) =>
      this.buildColumnDefinition(name, def)
    ).join(', ');

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
        const columnDefinition = this.buildColumnDefinition(columnName, columnDef);
        const addColumnSQL = `ALTER TABLE ${this.name} ADD COLUMN ${columnDefinition}`;

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

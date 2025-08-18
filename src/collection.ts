import postgres from 'postgres';
import * as zod from 'zod';
import { zodToMappedType, zodUnwrapType } from './typemap.js';
import { createEnumType, type ColumnDefinition } from './utils.js';

const DEFAULT_METHODS = ['now()', 'gen_random_uuid()',];

type InferRequiredId<T> = T extends { id?: infer U }
  ? Omit<T, 'id'> & { id: NonNullable<U> }
  : T;

export class Collection<T extends zod.core.$ZodLooseShape = any> {
  public 'Type': zod.infer<typeof this.zod>;
  public 'OutputType': InferRequiredId<this['Type']>;

  public name: string;

  public schema: {[name: string]: {
    type: string;
    default: string;
    nullable: boolean;
    maxLength: number | undefined;
    zodType: zod.ZodType;
  }} = {};

  protected zod: zod.ZodObject<T>;
  protected sql!: ReturnType<typeof postgres>;

  constructor(name: string, zodObject: zod.ZodObject<T>, sql: ReturnType<typeof postgres>) {
    this.name = name;
    this.zod = zodObject;
    this.sql = sql;
  }

  public parse(input: zod.input<typeof this.zod>): this['Type'] {
    return this.zod.parse(input);
  }

  public async insert<R = this['OutputType']>(
    strings: TemplateStringsArray,
    ...values: any[]
  ): Promise<R[]> {
    const newStrings = this.buildSqlTemplateStrings(
      strings,
      'INSERT INTO',
      /\b(ON\s+CONFLICT|RETURNING)\b/i, // SQL keywords that should come after the VALUES clause
      '',
      'RETURNING *'
    );

    const result = await this.sql(newStrings, ...processSQLValues(this.sql, values));

    return result.map((row: any, _: number) =>
      this.convertRowFromDatabase(row)) as R[];
  }

  public async create(input: zod.input<typeof this.zod>): Promise<this['OutputType']>;
  public async create(inputs: zod.input<typeof this.zod>[]): Promise<this['OutputType'][]>;
  public async create(inputOrInputs: zod.input<typeof this.zod> | zod.input<typeof this.zod>[]): Promise<this['OutputType'] | this['OutputType'][]> {
    const sql = this.sql;

    // handle array of inputs
    if (Array.isArray(inputOrInputs)) {
      const dataArray: any[] = [];

      // validate each input
      for (const input of inputOrInputs) {
        dataArray.push(this.parseWithDefaults(input));
      }

      if (dataArray.length === 0) {
        return [];
      }

      const results = await sql`INSERT INTO ${sql.unsafe(this.name)} ${sql(dataArray)} RETURNING *`;
      return results.map((result: any, index: number) => ({ ...result, ...inputOrInputs[index] })) as this['OutputType'][];

    } else {
      // handle single input
      const data = await this.zod.parse(inputOrInputs);
      const keys = Object.keys(data);

      const result = await sql`
        INSERT INTO ${sql.unsafe(this.name)}
        ${(keys.length > 0)
          ? sql(data as any, keys)
          : sql.unsafe('DEFAULT VALUES')}
        RETURNING *
      `;

      return { ...result[0], ...data } as this['OutputType'];
    }
  }

  public async select<R = this['OutputType']>(
    strings: TemplateStringsArray = Object.assign(["*"], { raw: ["*"] }),
    ...values: any[]
  ): Promise<R[]> {
    const newStrings = this.buildSqlTemplateStrings(
      strings,
      'SELECT',
      /\b(WHERE|ORDER\s+BY|GROUP\s+BY|HAVING|LIMIT|OFFSET)\b/i, // SQL keywords that should come after the FROM clause
      'FROM'
    );

    const result = await this.sql(newStrings, ...processSQLValues(this.sql, values));

    return result.map((row: any, _: number) =>
      this.convertRowFromDatabase(row)) as R[];
  }

  public async selectOne<R = this['OutputType']>(
    strings: TemplateStringsArray = Object.assign(["*"], { raw: ["*"] }),
    ...values: any[]
  ): Promise<R | undefined> {
    // Check if LIMIT is already present in the query
    let modifiedStrings = strings;

    if (/\blimit\b/i.test(strings.join(''))) {
      throw new Error(".selectOne() does not accept LIMIT");

    } else {
      // Add LIMIT 1 to the last string
      const newStrings = [...strings];
      const newRawStrings = [...strings.raw];

      newStrings[newStrings.length - 1] = (newStrings[newStrings.length - 1] || '') + ' LIMIT 1';
      newRawStrings[newRawStrings.length - 1] = (newRawStrings[newRawStrings.length - 1] || '') + ' LIMIT 1';

      modifiedStrings = Object.assign(newStrings, { raw: newRawStrings }) as TemplateStringsArray;
    }

    const results = await this.select(modifiedStrings, ...values);
    return results[0] as R | undefined;
  }

  public async update<R = this['OutputType']>(
    strings: TemplateStringsArray,
    ...values: any[]
  ): Promise<R[]> {
    const newStrings = this.buildSqlTemplateStrings(
      strings,
      'UPDATE',
      /\b(WHERE|ORDER\s+BY|LIMIT|OFFSET)\b/i, // SQL keywords that should come after the SET clause
      'SET',
      'RETURNING *'
    );

    const result = await this.sql(newStrings, ...processSQLValues(this.sql, values));

    return result.map((row: any, _: number) =>
      this.convertRowFromDatabase(row)) as R[];
  }

  public async delete(): Promise<number>;
  public async delete(strings: TemplateStringsArray, ...values: any[]): Promise<number>;
  public async delete<R>(strings: TemplateStringsArray, ...values: any[]): Promise<R[]>;
  public async delete<R = number>(
    strings: TemplateStringsArray = Object.assign([""], { raw: [""] }),
    ...values: any[]
  ): Promise<R | R[] | number> {
    const newStrings = this.buildSqlTemplateStrings(
      strings,
      'DELETE',
      /\b(WHERE|ORDER\s+BY|GROUP\s+BY|HAVING|LIMIT|OFFSET)\b/i, // SQL keywords that should come after the FROM clause,
      'FROM',
    );

    const result = await this.sql(newStrings, ...processSQLValues(this.sql, values));

    return (result.length > 0)
      ? [...result] as R[]
      : result.count as number;
  }

  public async count(
    strings: TemplateStringsArray = Object.assign([""], { raw: [""] }),
    ...values: any[]
  ): Promise<number> {
    const newStrings = this.buildSqlTemplateStrings(strings, 'SELECT COUNT(*)', /\b(WHERE|ORDER\s+BY|GROUP\s+BY|HAVING|LIMIT|OFFSET)\b/i, 'FROM');
    const result = await this.sql(newStrings, ...processSQLValues(this.sql, values));
    return (result[0] && parseInt(result[0].count)) ?? 0;
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
      const existingColumns = await this.columns();

      // Compare and alter table if needed
      await this.alterTable(existingColumns, zodSchema);
    }
  }

  public async drop() {
    await this.sql`DROP TABLE IF EXISTS ${this.sql.unsafe(this.name)}`;
  }

  public async columns() {
    return await this.sql`
        SELECT
          column_name, data_type, character_maximum_length, column_default, is_nullable
        FROM
          information_schema.columns
        WHERE
          table_name = ${this.name}
      `;
  }

  protected zodToTableSchema() {
    const schema: {[key: string]: ColumnDefinition} = {};

    for (const field in this.zod.shape) {
      schema[field] = this.zodToPostgresType(this.zod.shape[field] as zod.ZodType);
    }

    return schema;
  }

  protected zodToPostgresType(zodProperty: zod.ZodType): ColumnDefinition {
    const { type: currentType, nullable, defaultValue, unique } = zodUnwrapType(zodProperty);

    // Map Zod types directly to PostgreSQL types
    if (currentType instanceof zod.ZodString) {
      // Check for maxLength constraint
      if (currentType.maxLength !== null) {
        return { type: `VARCHAR(${currentType.maxLength})`, nullable, default: defaultValue, unique };
      }
      return { type: 'TEXT', nullable, default: defaultValue, unique };

    } else if (currentType instanceof zod.ZodNumber) {
      // Handle different number formats
      switch (currentType.format) {
        case 'float32':
          return { type: 'REAL', nullable, default: defaultValue, unique };
        case 'float64':
          return { type: 'DOUBLE PRECISION', nullable, default: defaultValue, unique };
        default:
          return { type: 'DECIMAL', nullable, default: defaultValue, unique };
      }

    } else if (currentType instanceof zod.ZodBoolean) {
      return { type: 'BOOLEAN', nullable, default: defaultValue, unique };

    } else if (currentType instanceof zod.ZodDate) {
      return { type: 'TIMESTAMP', nullable, default: (defaultValue) ? `now()` : null, unique };

    } else if (currentType instanceof zod.ZodArray) {
      return { type: 'JSONB', nullable, default: defaultValue, unique };

    } else if (currentType instanceof zod.ZodObject) {
      return { type: 'JSONB', nullable, default: defaultValue, unique };

    } else if (currentType instanceof zod.ZodRecord) {
      return { type: 'JSONB', nullable, default: defaultValue, unique };

    } else if (currentType instanceof zod.ZodMap) {
      return { type: 'JSONB', nullable, default: defaultValue, unique };

    } else if (currentType instanceof zod.ZodAny) {
      return { type: 'JSONB', nullable, default: defaultValue, unique };

    } else if (currentType instanceof zod.ZodEnum) {
      return { type: 'ENUM', nullable, default: defaultValue, options: currentType.options, unique };

    } else if (currentType instanceof zod.ZodLiteral) {
      return { type: 'TEXT', nullable, default: defaultValue, unique };

    } else if (currentType instanceof zod.ZodUnion) {
      // For unions, default to TEXT unless we can determine a more specific type
      return { type: 'TEXT', nullable, default: defaultValue, unique };

    } else if (currentType instanceof zod.ZodGUID || currentType instanceof zod.ZodUUID) {
      return { type: 'UUID', nullable, default: `gen_random_uuid()`, unique };

    } else {
      // Default fallback for unknown types
      return { type: 'TEXT', nullable, default: defaultValue, unique };
    }
  }

  protected buildColumnDefinition(name: string, def: ColumnDefinition): { def: string, updateDb?: () => Promise<void> } {
    let columnDef = name;
    let updateDb: any = undefined;

    if (def.type === 'ENUM') {
      // override the type to the enum type
      def.type = `${this.name}_${name}`;
      updateDb = () => createEnumType(this.sql, def);
    }

    if (name === 'id') {
      if (def.type === 'UUID') {
        columnDef += ` UUID PRIMARY KEY DEFAULT gen_random_uuid()`;

      } else {
        columnDef += ` INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY`;
      }

    } else {
      columnDef += ` ${def.type}`;

      if (!def.nullable) {
        columnDef += ' NOT NULL';
      }

      if (def.default !== undefined) {
        if (typeof def.default === 'string' && !DEFAULT_METHODS.includes(def.default)) {
          columnDef += ` DEFAULT '${def.default}'`;
        } else {
          columnDef += ` DEFAULT ${def.default}`;
        }
      }

      if (def.unique) {
        columnDef += ' UNIQUE';
      }
    }

    return { def: columnDef, updateDb };
  }

  protected async createTable(schema: {[key: string]: ColumnDefinition}) {
    let sqls: Array<() => Promise<void>> = [];

    const columns = Object.entries(schema).map(([name, def]) => {
      const { def: columnDef, updateDb } = this.buildColumnDefinition(name, def);
      if (updateDb) { sqls.push(updateDb); }
      return columnDef;
    }).join(', ');

    // Create ENUM types first
    for (const sql of sqls) {
      await sql();
    }

    const createTableSQL = `CREATE TABLE ${this.name} (${columns})`;
    // console.log({ createTableSQL });

    await this.sql.unsafe(createTableSQL);

    // Populate internal schema
    this.populateSchemaFromZod(schema);
  }

  protected async alterTable(existingColumns: any[], newSchema: {[key: string]: ColumnDefinition}) {
    const existingColumnMap = new Map();
    existingColumns.forEach(col => {
      existingColumnMap.set(col.column_name, {
        type: col.data_type,
        nullable: col.is_nullable === 'YES',
        default: col.column_default
      });
    });

    // Add new columns
    let alterTableCommands: string[] = [];
    let preAlterTableCommands: string[] = [];

    for (const [columnName, columnDef] of Object.entries(newSchema)) {
      const sqlDef = this.buildColumnDefinition(columnName, columnDef);

      if (sqlDef.updateDb) {
        await sqlDef.updateDb();
      }

      if (!existingColumnMap.has(columnName)) {
        alterTableCommands.push(`ADD COLUMN ${sqlDef.def}`);

      } else {
        const existingColumn = existingColumnMap.get(columnName);
        const newColumnDef = this.zodToPostgresType(this.zod.shape[columnName]);

        if (existingColumn.type !== zodToMappedType(columnName, this.zod.shape[columnName])) {
          let alterColumnCommand = `ALTER COLUMN ${columnName} TYPE ${columnDef.type}`;

          // Add explicit cast if the column is an enum
          if (newColumnDef.type === "ENUM") {
            alterColumnCommand += ` USING (${columnName}::${columnDef.type})`;
          }

          alterTableCommands.push(alterColumnCommand);

        } else if (columnName !== 'id' && existingColumn.nullable !== newColumnDef.nullable) {
          // Allow to switch from nullable to not nullable

          if (!newColumnDef.nullable) {
            if (newColumnDef.default === undefined) {
              // Explicitly throw an error if default value is not set
              throw new Error(`${this.name}: field '${columnName}' must have a default value`);

            } else {
              // Set default value if the column is not nullable
              preAlterTableCommands.push(`UPDATE ${this.name} SET ${columnName} = ${newColumnDef.default} WHERE ${columnName} IS NULL`);
            }
          }

          alterTableCommands.push(`ALTER COLUMN ${columnName} ${newColumnDef.nullable ? 'DROP NOT NULL' : 'SET NOT NULL'}`);
        }
      }
    }

    // console.log({ preAlterTableCommands, alterTableCommands });

    // Run pre-alter table commands first (to avoid constraint errors)
    if (preAlterTableCommands.length > 0) {
      await this.sql.unsafe(preAlterTableCommands.join('; '));
    }

    if (alterTableCommands.length > 0) {
      // console.log(`ALTER TABLE ${this.name} ${alterTableCommands.join(', ')}`);
      await this.sql.unsafe(`ALTER TABLE ${this.name} ${alterTableCommands.join(', ')}`);
    }

    // Populate internal schema with existing columns and new schema
    this.populateSchemaFromExisting(existingColumns);
    this.populateSchemaFromZod(newSchema);

    // Note: We're not dropping columns that exist in DB but not in schema
    // This is a safety measure to prevent data loss
  }

  protected populateSchemaFromZod(schema: {[key: string]: ColumnDefinition}) {
    for (const [columnName, columnDef] of Object.entries(schema)) {
      this.populateSchemaField(columnName, {
        type: columnDef.type,
        maxLength: this.extractMaxLengthFromType(columnDef.type),
        default: columnDef.default,
        nullable: columnDef.nullable,
      });
    }
  }

  protected populateSchemaFromExisting(existingColumns: any[]) {
    existingColumns.forEach((column) => {
      this.populateSchemaField(column.column_name, {
        type: column.data_type,
        maxLength: column.character_maximum_length || undefined,
        default: column.column_default,
        nullable: column.is_nullable === 'YES',
      });
    });
  }

  protected populateSchemaField(
    columnName: string,
    fieldData: { type: string; maxLength: number | undefined; default: any; nullable: boolean }
  ) {
    this.schema[columnName] = {
      type: fieldData.type,
      maxLength: fieldData.maxLength,
      default: fieldData.default,
      nullable: fieldData.nullable,
      zodType: (zodUnwrapType(this.zod.shape[columnName]).type),
    };
  }

  protected extractMaxLengthFromType(type: string): number | undefined {
    const match = type.match(/\((\d+)\)/);
    return match && match[1] ? parseInt(match[1], 10) : undefined;
  }

  protected convertRowFromDatabase(row: any): any {
    // Convert null values to undefined and handle type conversions
    return Object.fromEntries(
      Object.entries(row).map(([key, value]) => {
        if (value === null) {
          return [key, undefined];
        }

        // Get the Zod type for this field to determine if we need type conversion
        const innerType = this.schema[key]?.zodType;

        // Convert string numbers back to actual numbers for numeric fields
        if (innerType instanceof zod.ZodNumber && typeof value === 'string') {
          const numValue = Number(value);
          return [key, isNaN(numValue) ? value : numValue];
        }

        return [key, value];
      })
    );
  }

  protected buildSqlTemplateStrings(
    strings: TemplateStringsArray,
    prefix: string,
    keywordRegex: RegExp,
    insertKeyword: string,
    suffix: string = ''
  ): TemplateStringsArray {
    const firstString = strings[0] ?? (prefix === 'SELECT' ? "*" : "");
    const firstRawString = strings.raw[0] ?? (prefix === 'SELECT' ? "*" : "");

    // Check if the first string contains keywords that should come after the insert keyword
    const match = firstString.match(keywordRegex);

    let modifiedFirstString: string;
    let modifiedFirstRawString: string;

    if (match) {
      // Insert keyword before the first keyword that should come after it
      const keywordIndex = match.index!;
      const beforeKeyword = firstString.substring(0, keywordIndex).trim();
      const afterKeyword = firstString.substring(keywordIndex);

      if (insertKeyword === 'FROM') {
        modifiedFirstString = `${beforeKeyword} ${insertKeyword} ${this.name} ${afterKeyword}`;
      } else {
        modifiedFirstString = `${insertKeyword} ${beforeKeyword} ${afterKeyword}`;
      }

      const rawKeywordIndex = firstRawString.search(keywordRegex);
      const beforeKeywordRaw = firstRawString.substring(0, rawKeywordIndex).trim();
      const afterKeywordRaw = firstRawString.substring(rawKeywordIndex);

      if (insertKeyword === 'FROM') {
        modifiedFirstRawString = `${beforeKeywordRaw} ${insertKeyword} ${this.name} ${afterKeywordRaw}`;
      } else {
        modifiedFirstRawString = `${insertKeyword} ${beforeKeywordRaw} ${afterKeywordRaw}`;
      }
    } else {
      // No keywords found, handle based on the insert keyword
      if (insertKeyword === 'FROM') {
        modifiedFirstString = `${firstString} ${insertKeyword} ${this.name}`;
        modifiedFirstRawString = `${firstRawString} ${insertKeyword} ${this.name}`;
      } else {
        modifiedFirstString = `${insertKeyword} ${firstString}`;
        modifiedFirstRawString = `${insertKeyword} ${firstRawString}`;
      }
    }

    const finalStrings = suffix ? strings.slice(1, -1) : strings.slice(1);
    const finalRawStrings = suffix ? strings.raw.slice(1, -1) : strings.raw.slice(1);

    const lastString = suffix ? `${strings[strings.length - 1] || ""} ${suffix}` : "";
    const lastRawString = suffix ? `${strings.raw[strings.raw.length - 1] || ""} ${suffix}` : "";

    const newStrings = suffix
      ? [`${prefix} ${this.name} ${modifiedFirstString}`, ...finalStrings, lastString]
      : [`${prefix} ${modifiedFirstString}`, ...finalStrings];

    const newRawStrings = suffix
      ? [`${prefix} ${this.name} ${modifiedFirstRawString}`, ...finalRawStrings, lastRawString]
      : [`${prefix} ${modifiedFirstRawString}`, ...finalRawStrings];

    return Object.assign(newStrings, { raw: newRawStrings }) as TemplateStringsArray;
  }

  protected parseWithDefaults(input: zod.input<typeof this.zod>): this['Type'] {
    const withDefaults: any = this.zod.parse(input);

    for (const [fieldName, zodType] of Object.entries(this.zod.shape)) {
      if (!(fieldName in withDefaults) && fieldName !== 'id') {
        const { defaultValue, nullable } = zodUnwrapType(zodType as zod.ZodType);
        if (defaultValue !== undefined) {
          withDefaults[fieldName] = defaultValue;

        } else if (nullable) {
          withDefaults[fieldName] = this.sql.unsafe('DEFAULT');
        }
      }
    }

    return withDefaults as this['Type'];
  }

}

export function processSQLValues(sql: ReturnType<typeof postgres>, values: any[]): any[] {
    return values.map(value => {
        if (value instanceof Collection) {
            // Return the table name directly (no quotes)
            return sql.unsafe(value.name);
        }
        return value;
    });
}
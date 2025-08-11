# Zodgres

Database Collections for Postgres with type-safe schema validation using Zod.

## Features

- 🔒 **Type-safe** - Full TypeScript support with Zod schema validation
- 🚀 **Simple API** - Collection-based interface for common database operations
- 📦 **Flexible** - Works with Postgres or in-memory PGLite for testing
- ⚡ **SQL Templates** - Use SQL template literals for complex queries
- 🔄 **Auto-migration** - Automatic table creation from Zod schemas

## Installation

```bash
npm install zodgres
```

## Quick Start

```typescript
import { connect, z } from 'zodgres';

// Connect to database
const db = await connect('postgres://user:password@localhost:5432/mydb');

// Define a collection with Zod schema
const users = await db.collection('users', {
  id: z.number().optional(), // auto-incrementing
  name: z.string().max(100),
  age: z.number().min(0).max(100).optional(),
});

// Create records
const user = await users.create({ name: 'John Doe', age: 30 });
// Result: { id: 1, name: 'John Doe', age: 30 }

// Create multiple records
const newUsers = await users.create([
  { name: 'Alice' },
  { name: 'Bob', age: 25 }
]);
// Result: [{ id: 2, name: 'Alice' }, { id: 3, name: 'Bob', age: 25 }]

// Query records
const allUsers = await users.select(); // or users.select`*`
const adults = await users.select`* WHERE age >= ${18}`;

// Close connection
await db.close();
```

## API Overview

### Database Connection

#### `connect(uri, options?)`

Connect to a Postgres database or use in-memory storage for testing:

```typescript
// Connect to Postgres
const db = await connect('postgres://user:password@localhost:5432/mydb');

// Use in-memory database (great for testing)
const testDb = await connect(':memory:');
```

### Collection Definition

#### `db.collection(name, schema, params?)`

Create a type-safe collection with Zod schema validation:

```typescript
const items = await db.collection('items', {
  id: z.number().optional(),        // auto-incrementing primary key
  name: z.string().max(100),        // required string with max length
  price: z.number().positive(),      // required positive number
  description: z.string().optional(), // optional string
});
```

### Collection Operations

#### `create(data)` / `create(data[])`

Create single or multiple records:

```typescript
// Single record
const item = await items.create({
  name: 'Widget',
  price: 19.99
});

// Multiple records
const newItems = await items.create([
  { name: 'Gadget', price: 29.99 },
  { name: 'Tool', price: 39.99, description: 'Useful tool' }
]);
```

#### `select()` / `select``query` `

Query records using SQL template literals:

```typescript
// Select all records
const all = await items.select();

// Select with conditions
const expensive = await items.select`* WHERE price > ${25}`;
const byName = await items.select`* WHERE name = ${'Widget'}`;

// Complex queries
const recent = await items.select`
  name, price
  WHERE created_at > ${new Date('2024-01-01')}
  ORDER BY price DESC
  LIMIT ${10}
`;
```

#### `drop()`

Drop the collection table:

```typescript
await items.drop();
```

#### `migrate()`

Create or update the table schema:

```typescript
await items.migrate();
```

## Testing

The library supports in-memory databases for fast testing:

```typescript
import { connect, z } from '@colyseus/collection';

describe('My tests', () => {
  let db;

  before(async () => {
    db = await connect(':memory:'); // Uses PGLite
  });

  after(async () => {
    await db.close();
  });

  it('should create users', async () => {
    const users = await db.collection('users', {
      id: z.number().optional(),
      name: z.string(),
    });

    const user = await users.create({ name: 'Test User' });
    assert.deepStrictEqual(user, { id: 1, name: 'Test User' });
  });
});
```

## Schema Validation

All data is validated using Zod schemas before database operations:

```typescript
const products = await db.collection('products', {
  id: z.number().optional(),
  name: z.string().min(1).max(100),
  price: z.number().positive(),
  category: z.enum(['electronics', 'books', 'clothing']),
  tags: z.array(z.string()).optional(),
  metadata: z.record(z.any()).optional(),
});

// This will throw validation error
await products.create({
  name: '', // too short
  price: -10, // not positive
  category: 'invalid' // not in enum
});
```

## License

MIT

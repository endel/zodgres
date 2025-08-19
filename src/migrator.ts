import postgres from 'postgres';
import * as fs from 'fs';
import * as path from 'path';
import type { Database } from './database.js';
import type { Collection } from './collection.js';
import type { SQL } from './utils.js';

/**
 * Database migrator based on postgres-shift approach
 * Handles forward-only migrations with numerical prefixes
 */
export class Migrator {
  private sql: ReturnType<typeof postgres>;
  private migrationsPath: string | undefined;

  constructor(sql: ReturnType<typeof postgres>, migrationsPath: string | undefined) {
    this.sql = sql;
    this.migrationsPath = migrationsPath;
  }

  /**
   * Get migration files from a directory path
   */
  private getMigrationFiles(directoryPath: string): string[] {
    if (!fs.existsSync(directoryPath)) {
      return [];
    }

    return fs.readdirSync(directoryPath)
      .filter(file => {
        const filePath = path.join(directoryPath, file);
        const stat = fs.statSync(filePath);
        return stat.isFile() && /\.(ts|js)$/.test(file);
      })
      .sort(); // Alphabetical sort
  }

  /**
   * Get collection migration files from a collection-specific directory
   */
  private getCollectionMigrationFiles(collectionName: string): string[] {
    if (!this.hasMigrationFiles()) {
      return [];
    }

    const collectionMigrationsPath = path.join(this.migrationsPath!, collectionName);
    return this.getMigrationFiles(collectionMigrationsPath);
  }

  private hasMigrationFiles(): boolean {
    return this.migrationsPath !== undefined && fs.existsSync(this.migrationsPath);
  }

  /**
   * Run all pending global migrations (direct files in migrations folder)
   * Creates migrations table if it doesn't exist and runs migrations in order
   * Now runs before/after functions within a transaction, similar to collection migrations
   */
  async runGlobalMigrations(): Promise<void> {
    // Create migrations table if it doesn't exist
    await this.sql`
      CREATE TABLE IF NOT EXISTS migrations (
        id SERIAL PRIMARY KEY,
        migration VARCHAR(500) NOT NULL,
        collection VARCHAR(255),
        applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(migration, collection)
      )
    `;

    if (!this.hasMigrationFiles()) {
      return;
    }

    // Get list of applied migrations (global migrations have collection = NULL)
    const appliedMigrations = await this.sql`
      SELECT migration FROM migrations WHERE collection IS NULL ORDER BY migration
    `;
    const appliedMigrationPaths = new Set(appliedMigrations.map(row => row.migration));

    // Get list of global migration files (direct .ts/.js files in migrations folder)
    const migrationFiles = this.getMigrationFiles(this.migrationsPath!);

    // Run pending global migrations
    for (const migrationFile of migrationFiles) {
      const migrationFullPath = migrationFile;

      if (appliedMigrationPaths.has(migrationFullPath)) {
        continue; // Skip already applied migrations
      }

      const migrationPath = path.join(this.migrationsPath!, migrationFile);

      // console.log(`Running global migration: ${migrationFullPath}`);

      try {
        // Run global migration within a transaction
        await this.sql.begin(async (sql) => {
          await this.runSingleMigrationInTransaction(migrationPath, migrationFullPath, sql);

          // Record successful migration within the same transaction (global migration)
          await sql`
            INSERT INTO migrations (migration, collection)
            VALUES (${migrationFullPath}, NULL)
          `;
        });

        // console.log(`Global migration ${migrationFullPath} completed successfully`);
      } catch (error) {
        // console.error(`Global migration ${migrationFullPath} failed:`, error);
        throw error;
      }
    }
  }

  /**
   * Run complete collection migration within a single transaction
   * Includes before migrations, collection migrate, and after migrations
   */
  async migrateCollection(collectionName: string, collection: Collection, database: Database): Promise<void> {
    const hasMigrationFiles = this.hasMigrationFiles();

    return await this.sql.begin(async (sql) => {
      // Create a temporary collection instance that uses the transaction sql
      const transactionCollection = Object.create(collection);
      transactionCollection.sql = sql;

      let pendingMigrations: string[] = [];
      if (hasMigrationFiles) {
        // Get list of applied collection migrations
        const appliedMigrations = await sql`SELECT migration FROM migrations WHERE collection = ${collectionName} ORDER BY migration`;
        const appliedMigrationPaths = new Set(appliedMigrations.map(row => row.migration));

        // Get list of collection migration files
        pendingMigrations = this.getCollectionMigrationFiles(collectionName)
          .filter(file => !appliedMigrationPaths.has(file));

        // Run before migrations for pending migrations
        await this.runCollectionMigrationPhaseForFiles(collectionName, collection, database, 'before', pendingMigrations);
      }

      // Run collection migrate - get SQL commands and execute them within transaction
      const migrationSqls = await collection.getMigrationSql();
      for (const migrationSql of migrationSqls) {
        await sql.unsafe(migrationSql);
      }

      if (hasMigrationFiles) {
        // Run after migrations for pending migrations
        await this.runCollectionMigrationPhaseForFiles(collectionName, collection, database, 'after', pendingMigrations);

        // Record all pending migrations as applied
        for (const migrationFile of pendingMigrations) {
          await sql`
            INSERT INTO migrations (migration, collection)
            VALUES (${migrationFile}, ${collectionName})
          `;
        }
      }

    });
  }

  /**
   * Run collection migration phase for specific migration files
   */
  private async runCollectionMigrationPhaseForFiles(collectionName: string, collection: any, database: any, phase: 'before' | 'after', migrationFiles: string[]): Promise<void> {
    if (migrationFiles.length === 0) {
      return;
    }

    const collectionMigrationsPath = path.join(this.migrationsPath!, collectionName);

    // Run collection migrations
    for (const migrationFile of migrationFiles) {
      const migrationPath = path.join(collectionMigrationsPath, migrationFile);

      try {
        const migrationModule = await import(`file://${migrationPath}`);
        const migrationFunction = migrationModule[phase];

        if (typeof migrationFunction === 'function') {
          // console.log(`Running ${phase} migration for ${collectionName}: ${migrationFile}`);
          await migrationFunction(collection, database);
          // console.log(`${phase} migration for ${collectionName} completed: ${migrationFile}`);
        }
      } catch (error) {
        // console.error(`${phase} migration for ${collectionName} failed (${migrationFile}):`, error);
        throw error;
      }
    }
  }

  /**
   * Legacy method - now runs global migrations only
   * @deprecated Use runGlobalMigrations() instead
   */
  async runMigrations(): Promise<void> {
    return this.runGlobalMigrations();
  }

  /**
   * Run a single global migration within a transaction
   * Supports before/after pattern like collection migrations
   */
  private async runSingleMigrationInTransaction(
    migrationPath: string,
    migrationFullPath: string,
    sql: SQL
  ): Promise<void> {
    try {
      const migrationModule = await import(`file://${migrationPath}`);

      // Run 'before' function if it exists
      if (typeof migrationModule.before === 'function') {
        // console.log(`Running before hook for global migration: ${migrationFullPath}`);
        await migrationModule.before(sql);
      }

      // Run default migration function if it exists
      const migrationFunction = migrationModule.default || migrationModule;
      if (typeof migrationFunction === 'function' && migrationFunction !== migrationModule.before && migrationFunction !== migrationModule.after) {
        await migrationFunction(sql);
      }

      // Run 'after' function if it exists
      if (typeof migrationModule.after === 'function') {
        // console.log(`Running after hook for global migration: ${migrationFullPath}`);
        await migrationModule.after(sql);
      }

    } catch (error) {
      throw new Error(`Migration file execution failed: ${error}`);
    }
  }

  /**
   * Get list of applied migrations
   */
  async getAppliedMigrations(): Promise<Array<{ migration: string; collection: string | null; applied_at: Date }>> {
    try {
      return await this.sql<{ migration: string; collection: string | null; applied_at: Date }[]>`
        SELECT migration, collection, applied_at
        FROM migrations
        ORDER BY collection NULLS FIRST, migration
      `;
    } catch (error) {
      // If migrations table doesn't exist, return empty array
      return [];
    }
  }

    /**
   * Get list of pending migrations
   */
  async getPendingMigrations(): Promise<string[]> {
    if (!this.hasMigrationFiles()) {
      return [];
    }

    const appliedMigrations = await this.getAppliedMigrations();

    // Create a map of applied migrations by collection
    const appliedGlobalMigrations = new Set(
      appliedMigrations
        .filter(row => row.collection === null)
        .map(row => row.migration)
    );

    const appliedCollectionMigrations = new Map<string, Set<string>>();
    appliedMigrations
      .filter(row => row.collection !== null)
      .forEach(row => {
        if (!appliedCollectionMigrations.has(row.collection!)) {
          appliedCollectionMigrations.set(row.collection!, new Set());
        }
        appliedCollectionMigrations.get(row.collection!)!.add(row.migration);
      });

    // Get both global migration files and collection migration directories
    const globalMigrationFiles = this.getMigrationFiles(this.migrationsPath!);
    const migrationDirs = fs.readdirSync(this.migrationsPath!)
      .filter(dir => {
        const dirPath = path.join(this.migrationsPath!, dir);
        return fs.statSync(dirPath).isDirectory();
      })
      .sort();

    const pending: string[] = [];

    // Add pending global migrations
    for (const file of globalMigrationFiles) {
      if (!appliedGlobalMigrations.has(file)) {
        pending.push(`global: ${file}`);
      }
    }

    // Add pending collection migrations
    for (const dir of migrationDirs) {
      const collectionMigrationFiles = this.getCollectionMigrationFiles(dir);
      const appliedForCollection = appliedCollectionMigrations.get(dir) || new Set();

      for (const file of collectionMigrationFiles) {
        if (!appliedForCollection.has(file)) {
          pending.push(`${dir}: ${file}`);
        }
      }
    }

    return pending;
  }
}

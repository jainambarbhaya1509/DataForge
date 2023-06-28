const { Pool } = require("pg");
const prompt = require("prompt-sync")();

// PostgreSQL Server Information
const pool = new Pool({
    user: "postgres",
    host: "localhost",
    database: "test",
    password: "admin",
    port: 5432,
});

// Connect to PostgreSQL Database
async function connectToPostgreSQL() {
    try {
        await pool.connect();
        console.log('Connected to PostgreSQL');
    } catch (error) {
        console.error('Error connecting to PostgreSQL:', error);
    }
}

// Disconnect from PostgreSQL Database
async function disconnectFromPostgreSQL() {
    try {
        pool.end();
        console.log('Disconnected from PostgreSQL');
    } catch (error) {
        console.error('Error disconnecting from PostgreSQL:', error);
    }
}

/**
 * The `dataMapping` function prompts the user for schema, source table, source table columns,
 * destination table, destination table columns, and logical column names for normalization, and
 * performs various operations such as checking table and column existence, creating tables, mapping
 * data, and mapping distinct values.
 * @returns The function `dataMapping()` does not have a return statement.
 */
async function dataMapping() {

    const source_cols_dt = [];

    const schema = prompt("Enter schema: ");
    console.log();
    const source_table = prompt("Enter source table: ");
    const source_table_cols = prompt("Enter source table columns (comma-separated): ").replace(/\s+/g, "").split(",");

    for (const source_column of source_table_cols) {
        const sourceExists = await checkTable(schema, source_table, source_column);
        console.log();

        if (!sourceExists) {
            console.log("Schema, Table, or Column does not exist.");
            return;
        }
        else {
            const data_type = await getSourceDatatype(source_table, source_column, source_cols_dt)
            source_cols_dt.push(data_type);
        }
    }
    console.log();

    const destination_table = prompt("Enter destination table: ");
    const destination_table_cols = prompt("Enter destination table columns (comma-separated): ").replace(/\s+/g, "").split(",");

    for (const destination_column of destination_table_cols) {
        const destinationExists = await checkTable(schema, destination_table, destination_column);

        if (!destinationExists) {

            console.log("\nSchema, Table, or Column does not exist.");
            const askCreate = prompt(`Table "${destination_table}" does not exists. Do you want to create it (Y/n): `);
            console.log()

            if (askCreate === 'Y' || askCreate === 'y') {

                const tableCols = destination_table_cols.map((_, index) => `${destination_table_cols[index]} ${source_cols_dt[index]}`).join(", ");
                await createTable(schema, destination_table, tableCols)
            }
            else {
                return;
            }
        }
    }

    try {
        await mapData(destination_table, destination_table_cols, source_table_cols, source_table);
        console.log()
    } catch (error) {
        console.error("Error while mapping data: ", error)
        throw error
    }

    // Asking column name for normalization
    const normalizing_cols_dt = [];
    console.log()
    const normalize_cols = prompt("Enter logical column name for normalization (comma-separated): ").replace(/\s+/g, "").split(",");

    for (const normalize_col of normalize_cols) {
        const colsExists = await checkTable(schema, destination_table, normalize_col)

        if (!colsExists) {
            console.log(`"${normalize_col}" column does not exist`)
            return;
        }
    }
    for (const normalize_col of normalize_cols) {
        const data_type = await getSourceDatatype(destination_table, normalize_col, normalizing_cols_dt);
        normalizing_cols_dt.push(data_type);
        const tableCols = `${normalize_col} ${normalizing_cols_dt[normalizing_cols_dt.length - 1]}`;
        await createTable(schema, normalize_col, tableCols);
        await mapDistinctValue(normalize_col, normalize_col, normalize_col, destination_table);
    }
}

/**
 * This function checks if a specified column exists in a specified table within a specified schema in
 * a PostgreSQL database.
 * @param schema - The name of the database schema to check for the table and column.
 * @param table - The name of the table being checked for the existence of a specific column.
 * @param table_col - The name of the column being checked for existence in the specified table.
 * @returns a boolean value indicating whether a specific column exists in a table within a given
 * schema in a PostgreSQL database.
 */
async function checkTable(schema, table, table_col) {
    try {
        const client = await pool.connect();

        const query = `
        SELECT EXISTS (
          SELECT 1
          FROM information_schema.tables
          WHERE table_schema = '${schema}'
          AND table_name = '${table}'
        ) AND EXISTS (
          SELECT 1
          FROM information_schema.columns
          WHERE table_schema = '${schema}'
          AND table_name = '${table}'
          AND column_name = '${table_col}'
        ) AS column_exists;
      `;
        const result = await client.query(query);
        const columnExists = result.rows[0].column_exists;
        client.release();

        return columnExists;

    } catch (error) {
        console.error("Error checking the table: ", error);
        return false;
    }
}

/**
 * The function `mapData` inserts data from a source table into a destination table, only if the data
 * does not already exist in the destination table.
 * @param destination_table - The name of the destination table where the data will be inserted.
 * @param destination_table_cols - An array of column names in the destination table where the data
 * will be inserted.
 * @param source_table_cols - An array of column names from the source table that you want to map to
 * the destination table.
 * @param source_table - The source_table parameter is the name of the table from which you want to
 * select data to map into the destination table.
 */
async function mapData(destination_table, destination_table_cols, source_table_cols, source_table) {
    try {
        const client = await pool.connect();

        const destinationColumns = destination_table_cols.join(', ');
        const sourceColumns = source_table_cols.join(', ');

        const query = `INSERT INTO ${destination_table} (${destinationColumns})
        SELECT ${sourceColumns}
        FROM ${source_table}
        WHERE NOT EXISTS (
        SELECT 1
        FROM ${destination_table}
        WHERE (${destinationColumns}) = (${sourceColumns})
        )`;

        await client.query(query);
        client.release();
        console.log(`Data mapped into table "${destination_table}"`)
    } catch (error) {
        console.error("Error mapping the data: ", error);
        throw error;
    }
}

/**
 * This function creates a table in a specified schema and destination table with specified columns.
 * @param schema - The name of the database schema where the table will be created.
 * @param destination_table - The name of the table that will be created or checked if it already
 * exists.
 * @param tableCols - The tableCols parameter is a string that contains the column names and their data
 * types for the table being created. For example, it could be "id SERIAL PRIMARY KEY, name
 * VARCHAR(255), age INT".
 */
async function createTable(schema, destination_table, tableCols) {
    try {
        const client = await pool.connect();
        const query = `CREATE TABLE IF NOT EXISTS ${schema}.${destination_table} ( ${tableCols} );`;
        await client.query(query);
        client.release();
        console.log(`Destination table "${destination_table}" created`)
    } catch (error) {
        throw error;
    }
}

/**
 * This function retrieves the data type of a specified column in a specified table from a PostgreSQL
 * database.
 * @param source_table - The name of the table in the PostgreSQL database from which you want to
 * retrieve the data type of a specific column.
 * @param source_column - The name of the column in the source table whose data type is to be
 * retrieved.
 * @returns the data type of a specified column in a specified table in a PostgreSQL database.
 */
async function getSourceDatatype(source_table, source_column) {
    try {
        const client = await pool.connect();
        query = `SELECT attname, format_type(atttypid, atttypmod) AS data_type
        FROM pg_attribute
        WHERE attrelid = '${source_table}'::regclass
        AND attname = '${source_column}'
        AND attnum > 0;
        `
        const result = await client.query(query);
        const data_type = result.rows[0].data_type;

        client.release()

        return data_type

    } catch (error) {
        console.log(error)
        throw error;
    }
}

/**
 * The function maps distinct values from a source table to a destination table, ensuring that the
 * values are not already present in the destination table.
 * @param normalization_table - The name of the table where the distinct values will be inserted.
 * @param normalization_table_cols - The `normalization_table_cols` parameter is a string that
 * represents the columns in the normalization table that you want to insert the distinct values into.
 * @param destination_column - The `destination_column` parameter is the name of the column in the
 * `destination_table` from which distinct values will be selected and inserted into the
 * `normalization_table`.
 * @param destination_table - The `destination_table` parameter refers to the name of the table from
 * which you want to select distinct values for mapping.
 */
async function mapDistinctValue(normalization_table, normalization_table_cols, destination_column, destination_table) {
    try {
        const client = await pool.connect();
        const query = `INSERT INTO ${normalization_table} (${normalization_table_cols})
        SELECT DISTINCT ${destination_column}
        FROM ${destination_table}
        WHERE NOT EXISTS (
        SELECT 1
        FROM ${normalization_table}
        WHERE ${normalization_table}.${normalization_table_cols} = ${destination_table}.${destination_column}
        )`;

        await client.query(query);
        client.release();
    } catch (error) {
        console.log("Error mapping the data:", error);
        throw error
    }
}

async function createReference() {

}
// Main function
async function main() {
    try {
        await connectToPostgreSQL();
        await dataMapping();
    } catch (error) {
        console.error('Error:', error);
    } finally {
        await disconnectFromPostgreSQL();
    }
}

// Call the main function
main();
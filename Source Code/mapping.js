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

/*
This function prompts the user to enter schema, source table, source table columns, destination
table, and destination table columns, checks if they exist, and maps data from the source table to
the destination table.

It contains an async function that prompts the user for input, checks if certain tables 
and columns exist, and maps data from a source table to a destination table.
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
            const askCreate = prompt(`Table ${destination_table} does not exists. Do you want to create it (Y/n): `);

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
    } catch (error) {
        console.error("Error while mapping data: ", error)
        throw error
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
 * This function maps data from a source table to a destination table using specified columns.
 * @param destination_table - The name of the table where the data will be inserted.
 * @param destination_table_cols - An array of column names in the destination table where the data
 * will be inserted.
 * @param source_table_cols - An array of column names from the source table that will be selected in
 * the query.
 * @param source_table - The name of the table from which data is being selected.
 */
async function mapData(destination_table, destination_table_cols, source_table_cols, source_table) {
    try {
        const client = await pool.connect();

        const destinationColumns = destination_table_cols.join(', ');
        const sourceColumns = source_table_cols.join(', ');

        const query = `INSERT INTO ${destination_table} (${destinationColumns})
                       SELECT ${sourceColumns}
                       FROM ${source_table}`;

        await client.query(query);
        client.release();
    } catch (error) {
        console.error("Error mapping the data: ", error);
    }
}

async function createTable(schema, destination_table, tableCols) {
    try {
        const client = await pool.connect();
        const query = `CREATE TABLE IF NOT EXISTS ${schema}.${destination_table} ( ${tableCols} );`;
        console.log(query);
        await client.query(query);
        client.release();
    } catch (error) {
        throw error;
    }
}

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
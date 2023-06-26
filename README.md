# PostgreSQL Data Mapping
This code provides functionality for mapping data from a source table to a destination table in a PostgreSQL database. It prompts the user for input to specify the schema, tables, and columns involved in the data mapping process. It also checks the existence of tables and columns, creates the destination table if it does not exist, and performs the data mapping operation.

## Prerequisites
- Node.js
- PostgreSQL

## Installation
1. Clone the repository or download the code files.
    ```
    https://github.com/jainambarbhaya1509/Data-Mapping.git
    ```

2. Open a terminal or command prompt and navigate to the project directory.

3. Run the following command to install the required dependencies:
    ```
    npm install pg prompt-sync
    ```

## Usage
1. Open the code file in a text editor.

2. Modify the PostgreSQL server information in the code to match your server configuration:
    ```javascript
    const pool = new Pool({
        user: "postgres",
        host: "localhost",
        database: "test",
        password: "admin",
        port: 5432,
    });
    ```

3. Save the changes to the file.

4. Open a terminal or command prompt and navigate to the project directory.

5. Run the following command to execute the code:
    ```
    node mapping.js
    ```

6. Follow the prompts to enter the schema, source table, source table columns, destination table, and destination table columns.

7. The code will check if the specified tables and columns exist. If any of them do not exist, it will prompt you to create the destination table.

8. The code will then map the data from the source table to the destination table using the specified columns.

## Contributing
Contributions to this repository are welcome! If you encounter any issues or have suggestions for improvements, please open an issue or submit a pull request. Your contributions can help make this script more robust and usable in various scenarios.

When contributing, please adhere to the following guidelines:

- Clearly describe the problem or feature you are addressing in your pull request or issue.
- Provide a detailed explanation of the changes you have made.
- Test your changes thoroughly to ensure they do not introduce any regressions.
- Follow the existing coding style and conventions used in the project.

Thank you for your interest in this repository. Your contributions are greatly appreciated!

## Disclaimer
The code provided is a data mapping script for migrating data into a PostgreSQL database. It may require modifications to suit specific use cases and does not cover all scenarios. The author and contributors cannot be held liable for any issues or damages resulting from its use. Test the code thoroughly and consult professionals when working with critical data. Use at your own risk.

## License
This project is licensed under the [MIT License](https://opensource.org/licenses/MIT).

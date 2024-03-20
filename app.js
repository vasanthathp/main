const express  = require('express');
const { DBSQLClient } = require('@databricks/sql');

const app = express();
const port = process.env.PORT || 5001;

// Load credentials from environment variables (replace with yours)
const serverHostname = "adb-652784074740646.6.azuredatabricks.net";
const httpPath = "/sql/1.0/warehouses/7a17086b79f6dff5";
const token = "dapi794ffac1d1a731d491462bb1df412561-3";

// Create a Databricks SQL client
const client = new DBSQLClient();

// Connect to Databricks SQL Warehouse
client.connect({
    token,
    host: serverHostname,
    path: httpPath,
})
    .then(() => console.log('Connected to Databricks SQL Warehouse!'))
    .catch((error) => console.error('Connection error:', error));

// Define an API endpoint to execute a sample query
app.get('/api/data', async (req, res) => {
    try {

        const session = await client.openSession();
        const queryOperation = await session.executeStatement(
            'select * from samples.tpch.customer limit 100',
            {
                runAsync: true,
                maxRows: 10000 // This option enables the direct results feature.
            }
        );

        const result = await queryOperation.fetchAll();
        res.json(result);

        await queryOperation.close();

        await session.close();
        await client.close();

    } catch (error) {
        console.error('Error executing query:', error);
        res.status(500).send('Error retrieving data');
    }
});

app.listen(port, () => console.log(`Server listening on port ${port}`));
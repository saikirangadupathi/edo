const express = require('express');

const cors=require('cors')


const app = express();
const port = 5000;


app.use(cors());

// Replace with your Apify actor ID and API token

const apiToken = 'apify_api_kw0E2396DY8edB2EN4j9GcxFZzmhtF17fgCl';

// Middleware to parse JSON in the request body
app.use(express.json());

// ////////////




const { BigQuery } = require('@google-cloud/bigquery');

const { MetricServiceClient } = require('@google-cloud/monitoring');


// Create a client

// Set the service account key file path
const keyFilePath = './keyfile.json';

// Create a client with the key file path
const bigquery = new BigQuery({
  keyFilename: keyFilePath,
});



// 
async function getStorageMetrics() {


    const projectId = 'my-new-project-4-412310';
    const metricServiceClient = new MetricServiceClient( {keyFilename: keyFilePath});
  
    const [timeSeries] = await metricServiceClient.listTimeSeries({
      name: `projects/${projectId}`,
      filter: 'metric.type="storage.googleapis.com/storage/total_bytes"',
      interval: {
        startTime: {
          seconds: Date.now() / 1000 - 3600, // Start time (1 hour ago)
        },
        endTime: {
          seconds: Date.now() / 1000, // End time (now)
        },
      },
    });
  
    console.log('Time Series:', timeSeries);
  }
  

// Function to get the total storage consumed by datasets in a project// Function to get the total storage consumed by datasets in a project
const projectId = 'my-new-project-4-412310';

async function getTotalStorageConsumed() {
  try {
    // Get datasets in the project
    const [datasets] = await bigquery.getDatasets({ projectId });

    let totalStorageBytes = 0;

    console.log("datasets",datasets)

    // Iterate through datasets
    for (const dataset of datasets) {
      const datasetId = dataset.id;
      
      // Get tables in the dataset
      const [tables] = await bigquery.dataset(datasetId).getTables();
      
      // Iterate through tables and sum up their sizes
      for (const table of tables) {
        const [tableMetadata] = await table.getMetadata();

        console.log("data",tableMetadata,"tables",tables)

        console.log("tablemetadata.numBytes,dataset",tableMetadata.numBytes,dataset)
        
        if (tableMetadata.numBytes && tableMetadata.numBytes) {
          totalStorageBytes += parseInt(tableMetadata.numBytes)
        }
      }
    }

    // Convert bytes to gigabytes
    const totalStorageGB = totalStorageBytes / (1024 * 1024 * 1024);

    console.log(`Total storage consumed in project ${projectId}: ${totalStorageGB.toFixed(2)} GB`);

    console.log(`Total storage consumed in project ${projectId}: ${(totalStorageBytes / (1024 * 1024)).toFixed(2)} MB`);
    
    console.log(`Total storage consumed in project ${projectId}: ${totalStorageBytes} BYTES`);
  } catch (err) {
    console.error('Error fetching storage data:', err);
  }
}
  
  

app.get('/bigquery', async (req, res) => {


    try {
      // Define your query
      const query = 'SELECT * FROM `my-new-project-4-412310.dataset4.table4` LIMIT 10';
  
      // Run the query
      const [results] = await bigquery.query(query);
  
      // Extract rows from the results
      const rows = results[0];
      console.log('Rows:', rows);
  
      // Send the entire array of rows as JSON
      res.json(rows);

      getTotalStorageConsumed()

    } catch (error) {
      console.error('Error:', error.message || error);
      res.status(500).json({ success: false, error: error.message || error });
    }
  });
  












// Start the Express server
app.listen(port, () => {
    console.log(`Server is running on http://localhost:${port}`);
});

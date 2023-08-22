const fs = require('fs');
const SparqlParser = require('sparqljs').Parser;
const csvParser = require('csv-parser');
const { unparse } = require('papaparse');
const { Worker, isMainThread, parentPort } = require('worker_threads');
const pMap = require('p-map');

// Create a new instance of the SPARQL parser
const parser = new SparqlParser();

// Function to execute a single SPARQL query and return the result
async function executeQuery(query) {
  try {
    const parsedQuery = parser.parse(query);
    // Here, you can execute the parsed query using your preferred SPARQL endpoint or RDF library
    // For the sake of example, we will just return the parsed query as a string
    return JSON.stringify(parsedQuery);
  } catch (error) {
    console.error('Error parsing query:', query);
    return 'Error parsing the query.';
  }
}

async function main() {
  // Read SPARQL queries from input CSV file and execute them in parallel
  const inputCsvFile = 'unique_queries.csv';
  const outputCsvFile = 'parsed_sample_parallel.csv';

  const csvData = [];

  // Step 1: Count the total number of queries in the input CSV file
  let totalQueries = 0;
  fs.createReadStream(inputCsvFile)
    .pipe(csvParser())
    .on('data', () => {
      totalQueries++;
    })
    .on('end', () => {
      console.log(`Total SPARQL queries to process: ${totalQueries}`);
    });

  const workerFunction = async (row) => {
    const sparqlQuery = row['query']; // Adjust the column name as per your input CSV format
    const parsedQuery = await executeQuery(sparqlQuery);

    // Add the results to the csvData array
    csvData.push({ Query: sparqlQuery, Parsed_Query: parsedQuery });

    // Calculate the completion percentage and show the progress
    const completionPercentage = (csvData.length / totalQueries) * 100;
    console.log(`Progress: ${completionPercentage.toFixed(2)}%`);

    return { Query: sparqlQuery, Parsed_Query: parsedQuery };
  };

  // Use pMap to parallelize the execution of queries
  const workerCount = 4; // Adjust the number of worker threads as per your CPU cores
  await pMap(
    fs.createReadStream(inputCsvFile).pipe(csvParser()),
    workerFunction,
    { concurrency: workerCount }
  );

  // Write the results to output CSV file
  const csv = unparse(csvData, { header: true, delimiter: ',' });
  fs.writeFileSync(outputCsvFile, csv);

  console.log('All SPARQL queries executed and results written to output CSV file.');
}

if (isMainThread) {
  main();
} else {
  parentPort.once('message', (message) => {
    parentPort.postMessage(workerFunction(message));
  });
}

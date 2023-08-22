const fs = require('fs');
const SparqlParser = require('sparqljs').Parser;
const csvParser = require('csv-parser');
const { stringify } = require('csv-stringify');

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
  // Read SPARQL queries from input CSV file and execute them one by one
  const inputCsvFile = 'unique_queries.csv';
  const outputCsvFile = '_yyy.csv';

  const batchSize = 10; // Number of queries to accumulate before writing to CSV

  const csvData = [];
  let processedQueries = 0;

  console.log('Start reading and processing SPARQL queries...');

  const readStream = fs.createReadStream(inputCsvFile).pipe(csvParser());

  readStream.on('data', async (row) => {
    const sparqlQuery = row['query']; // Adjust the column name as per your input CSV format

    // Execute SPARQL query and collect the result
    const parsedQuery = await executeQuery(sparqlQuery);

    // Add the result to the csvData array
    csvData.push({ Query: sparqlQuery, Parsed_Query: parsedQuery });

    processedQueries++;

    // If the batch size is reached, write the batch to the output CSV file
    if (processedQueries % batchSize === 0) {
      await writeBatchToCsv(csvData, outputCsvFile);
      console.log(`Processed ${processedQueries} queries.`);
      csvData.length = 0; // Clear the array
    }
  });

  readStream.on('end', async () => {
    // Write any remaining queries in the array to the output CSV file
    if (csvData.length > 0) {
      await writeBatchToCsv(csvData, outputCsvFile);
    }

    console.log('All SPARQL queries executed and results written to output CSV file.');
  });

  readStream.on('error', (error) => {
    console.error('An error occurred while reading the input CSV file:', error);
  });
}

async function writeBatchToCsv(batch, outputFile) {
  return new Promise((resolve, reject) => {
    const writeStream = fs.createWriteStream(outputFile, { flags: 'a' }); // 'a' for append mode
    const csvStringifier = stringify({ header: false, delimiter: ',' });

    writeStream.on('error', (error) => {
      reject(error);
    });

    csvStringifier.on('readable', () => {
      let row;
      while ((row = csvStringifier.read())) {
        writeStream.write(row);
      }
    });

    csvStringifier.on('end', () => {
      writeStream.end();
      resolve();
    });

    batch.forEach((entry) => {
      csvStringifier.write([entry.Query, entry.Parsed_Query]);
    });

    csvStringifier.end();
  });
}

main();
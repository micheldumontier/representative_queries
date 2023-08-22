const fs = require('fs');
const SparqlParser = require('sparqljs').Parser;
const csvParser = require('csv-parser');
const { unparse } = require('papaparse');

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
  const inputCsvFile = 'prefixes_added_bio2rdf.csv';
  const outputCsvFile = 'parsed_out_yyy_prefix.csv';

  const csvData = [];

  console.log('Start reading and processing SPARQL queries...');

  const readStream = fs.createReadStream(inputCsvFile).pipe(csvParser());

  let queryCount = 0;

  readStream.on('data', async (row) => {
    const sparqlQuery = row['query']; // Adjust the column name as per your input CSV format

    // Execute SPARQL query and collect the result
    const parsedQuery = await executeQuery(sparqlQuery);

    // Add the result to the csvData array
    csvData.push({ Query: sparqlQuery, Parsed_Query: parsedQuery });

    queryCount++;

    // Print progress after processing every 500,000 queries
    if (queryCount % 300000 === 0) {
      console.log(`Processed ${queryCount} queries.`);
    }
  });

  readStream.on('end', () => {
    // Write the result to output CSV file after all queries are processed
    const csv = unparse(csvData, { header: true, delimiter: ',' });
    fs.writeFileSync(outputCsvFile, csv);

    console.log('All SPARQL queries executed and results written to output CSV file.');
  });

  readStream.on('error', (error) => {
    console.error('An error occurred while reading the input CSV file:', error);
  });
}

main();

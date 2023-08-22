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
  const inputCsvFile = 'unique_queries.csv';
  const outputCsvFile = '_yyy.csv';

  const csvData = [];

  // Use stream processing to read rows from the input CSV file
  const stream = fs.createReadStream(inputCsvFile)
    .pipe(csvParser({ headers: true }));

  // Process each row using the stream
  for await (const row of stream) {
    const sparqlQuery = row['query'];

    const parsedQuery = await executeQuery(sparqlQuery);

    csvData.push({ Query: sparqlQuery, Parsed_Query: parsedQuery });
  }

  // Write all data to the output CSV file in one go
  const csv = unparse(csvData, { header: true, delimiter: ',' });
  fs.writeFileSync(outputCsvFile, csv);

  console.log('All SPARQL queries executed and results written to output CSV file.');
}

main();

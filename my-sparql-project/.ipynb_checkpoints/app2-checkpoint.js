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
  const inputCsvFile = 'unique_bio2rdf_sparql_logs.csv';
  const outputCsvFile = 'translate_unique_bio2rdf_sparql_logs.csv';

  const csvData = [];
  fs.createReadStream(inputCsvFile)
    .pipe(csvParser())
    .on('data', async (row) => {
      const sparqlQuery = row['query']; // Adjust the column name as per your input CSV format

      // Execute SPARQL query and collect the result
      const parsedQuery = await executeQuery(sparqlQuery);

      // Add the result to the csvData array
      csvData.push({ Query: sparqlQuery, Parsed_Query: parsedQuery });

      // Write the result to output CSV file after each query
      const csv = unparse(csvData, { header: true, delimiter: ',' });
      fs.writeFileSync(outputCsvFile, csv);

      console.log('SPARQL query executed and result written to output CSV file.');
    })
    .on('end', () => {
      console.log('All SPARQL queries executed and results written to output CSV file.');
    })
    .on('error', (error) => {
      console.error('An error occurred while reading the input CSV file:', error);
    });
}

main();

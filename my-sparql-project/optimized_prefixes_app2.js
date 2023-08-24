const fs = require('fs');
const fastcsv = require('fast-csv');
const SparqlParser = require('sparqljs').Parser;
const util = require('util');

// Create a new instance of the SPARQL parser
const parser = new SparqlParser();

// Paths to input CSV file and output file
const inputFilePath = 'unique_queries.csv';  // Replace with your input CSV file path
const outputFilePath = 'y.csv'; // Replace with your output file path

// Create a readable stream from the CSV file
const csvStream = fs.createReadStream(inputFilePath);

// Create a writable stream for the output file
const outputStream = fs.createWriteStream(outputFilePath);

// Create a CSV parser stream
const csvParser = fastcsv.parse({ headers: true });

// Pipe the CSV stream to the CSV parser
csvStream.pipe(csvParser);

// Process each row of the CSV file
csvParser.on('data', async (row) => {
  const sparqlQuery = row.query; // Assuming 'query' is the column name in the CSV

  // Parse the SPARQL query
  const parsedQuery = parser.parse(sparqlQuery);

  // Update variables and triples to show actual values
  parsedQuery.variables = displayVariables(parsedQuery.variables);
  parsedQuery.where[0].triples = parsedQuery.where[0].triples.map(triple => displayVariables(triple));

  // Convert the parsed query object to a string
  const parsedQueryStr = util.inspect(parsedQuery, { depth: null, colors: false });

  // Write the original query and parsed query to the output file
  await new Promise((resolve) => {
    outputStream.write(`Original Query:\n${sparqlQuery}\n\nParsed Query:\n${parsedQueryStr}\n\n`, resolve);
  });
});

// Close the output stream after processing all rows
csvParser.on('end', () => {
  outputStream.end();
});

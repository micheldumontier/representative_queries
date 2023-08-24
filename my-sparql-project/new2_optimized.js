const fs = require('fs');
const SparqlParser = require('sparqljs').Parser;
const csvParser = require('csv-parser');
const { stringify } = require('csv-stringify');

const parser = new SparqlParser();

async function executeQuery(query) {
  try {
    const parsedQuery = parser.parse(query);
    return JSON.stringify(parsedQuery);
  } catch (error) {
    console.error('Error parsing query:', query);
    return 'Error parsing the query.';
  }
}

async function main() {
  // const inputCsvFile = '../data/unique_bio2rdf_sparql_logs_with_counts.csv';
  // const outputCsvFile = 'parsed_queries_all.csv';
  // const inputCsvFile = 'unique_queries.csv';
  // const outputCsvFile = '_yyy.csv';
  const inputCsvFile = '../data/prefixes_added_bio2rdf.csv';
  const outputCsvFile = 'prefix_parsed_queries_all.csv';
  const batchSize = 1000;

  const readStream = fs.createReadStream(inputCsvFile).pipe(csvParser());

  let batch = [];
  for await (const row of readStream) {
    const sparqlQuery = row['query'];

    const parsedQuery = await executeQuery(sparqlQuery);

    batch.push({ Query: sparqlQuery, Parsed_Query: parsedQuery });

    if (batch.length >= batchSize) {
      await writeBatchToCsv(batch, outputCsvFile);
      console.log(`Processed ${batch.length} queries.`);
      batch = [];
    }
  }

  if (batch.length > 0) {
    await writeBatchToCsv(batch, outputCsvFile);
  }

  console.log('All SPARQL queries executed and results written to output CSV file.');
}

async function writeBatchToCsv(batch, outputFile) {
  return new Promise((resolve, reject) => {
    const writeStream = fs.createWriteStream(outputFile, { flags: 'a' });
    const csvStringifier = stringify({ header: false, delimiter: ',' });

    writeStream.on('error', (error) => {
      reject(error);
    });

    csvStringifier.pipe(writeStream);

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

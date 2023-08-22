const SparqlParser = require('sparqljs').Parser;
const util = require('util');

// Create a new instance of the SPARQL parser
const parser = new SparqlParser();

// Example SPARQL query
// const sparqlQuery = `

//     SELECT ?x ?y WHERE {
//   ?x a ?y
//   {
//     SELECT ?y WHERE { ?y ?o ?d  }
//   }  # \subQuery
// } # \mainQuery
// `;


// second test
// const sparqlQuery = `PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>PREFIX owl: <http://www.w3.org/2002/07/owl#>PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>PREFIX foaf: <http://xmlns.com/foaf/0.1/>PREFIX dc: <http://purl.org/dc/elements/1.1/>PREFIX dcterms: <http://purl.org/dc/terms/>PREFIX skos: <http://www.w3.org/2004/02/skos/core#>PREFIX schema: <http://schema.org/>PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>SELECT ?s WHERE {   ?s ?p ?o . FILTER(STRSTARTS(STR(?s), "http://drugbank.bio2rdf.org/fct/"))  } LIMIT 1`;

// property path test
// const sparqlQuery = `select * where{
//   ?x <example:www/mbox> <mailto:alice@example> .
//   ?x <foaf:knows>+/<foaf:name> ?name .
//  }`;

// optional test
// const sparqlQuery = `PREFIX info:    <http://somewhere/peopleInfo#>
// PREFIX vcard:   <http://www.w3.org/2001/vcard-rdf/3.0#>

// SELECT ?name ?age
// WHERE
// {
//     ?person vcard:FN  ?name .
//     OPTIONAL { ?person info:age ?age }
// }`;

// union test
const sparqlQuery = `PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?page ?type WHERE
{
    ?s foaf:page ?page .
    { ?s rdfs:label "Microsoft"@en . BIND ("A" as ?type) }
    UNION
    { ?s rdfs:label "Apple"@en . BIND ("B" as ?type) }
}`;


// Parse the SPARQL query
const parsedQuery = parser.parse(sparqlQuery);

// Helper function to display the variable names
function displayVariables(variables) {
  if (Array.isArray(variables)) {
    return variables.map(v => v.value).join(' ');
  } else {
    // If variables is a Wildcard object, return it as is
    return variables;
  }
}

// Update variables and triples to show actual values
parsedQuery.variables = displayVariables(parsedQuery.variables);
parsedQuery.where[0].triples = parsedQuery.where[0].triples.map(triple => displayVariables(triple));

// Use util.inspect to see the actual values
console.log(util.inspect(parsedQuery, { depth: null, colors: true }));

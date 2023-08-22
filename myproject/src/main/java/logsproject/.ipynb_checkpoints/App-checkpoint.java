package logsproject;

/import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;

public class CountTriplePatterns {

    public static int countTriplePatterns(String sparqlQuery) {
        try {
            Query query = QueryFactory.create(sparqlQuery);
            return query.getQueryPattern().getList().size();
        } catch (Exception e) {
            return 0;
        }
    }

    public static void main(String[] args) {
        String sparqlQuery = "SELECT DISTINCT ?uri WHERE { ?x <http://dbpedia.org/property/international> <http://dbpedia.org/resource/Muslim_Brotherhood> . ?x <http://dbpedia.org/ontology/religion> ?uri . ?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/PoliticalParty> }";

        int numTriplePatterns = countTriplePatterns(sparqlQuery);
        System.out.println("Number of triple patterns: " + numTriplePatterns);
    }
}
package net.soldaini.terrierServer;

import java.io.*;
import javax.xml.parsers.ParserConfigurationException;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.json.JSONObject;
import spark.Spark;
import org.terrier.matching.ResultSet;
import org.terrier.querying.Manager;
import org.terrier.structures.*;
import org.terrier.querying.SearchRequest;
import org.terrier.utility.ApplicationSetup;
import org.xml.sax.SAXException;
import spark.Request;
import spark.Response;
import java.lang.String;
import java.util.*;
import org.eclipse.jetty.http.pathmap.PathSpec;


class TerrierCore {
    private Index index;
    private PostingIndex <Pointer> invertedIndex;
    private DocumentIndex documentIndex;
    private MetaIndex metaIndex;
    private Lexicon<String> lex;
    private org.terrier.terms.PorterStemmer stemmer;
    private HashMap <String, String> initialDefaultProperties;

    public String getProperty(String propName){
        return initialDefaultProperties.get(propName);
    }

    public String getPropertyOrDefault(String propName, String defaultValue){
        return initialDefaultProperties.getOrDefault(propName, defaultValue);
    }

    public TerrierCore(){
        index = Index.createIndex();
        invertedIndex = (PostingIndex<Pointer>) index.getInvertedIndex();
        documentIndex = index.getDocumentIndex();
        metaIndex = index.getMetaIndex();
        lex = index.getLexicon();
        stemmer = new org.terrier.terms.PorterStemmer();
        initialDefaultProperties = new HashMap<>();

        // as some of the properties specified in the application
        // setup might change during computation (e.g. number of retrieved
        // results @ query time), we preserve the original run properties
        // here and restore them if changed during request

        Properties properties = ApplicationSetup.getProperties();
        Enumeration<String> propEnum = (Enumeration<String>) properties.propertyNames();
        while (propEnum.hasMoreElements()) {
            String propName = propEnum.nextElement();
            initialDefaultProperties.put(propName, properties.getProperty(propName));
        }

        Properties usedProperties = ApplicationSetup.getUsedProperties();
        Enumeration<String> usedPropEnum = (Enumeration<String>) usedProperties.propertyNames();
        while (usedPropEnum.hasMoreElements()) {
            String propName = usedPropEnum.nextElement();
            initialDefaultProperties.put(propName, usedProperties.getProperty(propName));
        }
    }

    private void restoreProperties (Set <String> modifiedProperties) {
        for (String propName : modifiedProperties) {
            if (initialDefaultProperties.containsKey(propName)) {
                ApplicationSetup.setProperty(propName, initialDefaultProperties.get(propName));
            }
        }
    }

    public CollectionStatistics getCollectionStatistics() {
        return index.getCollectionStatistics();
    }

    public void close() {
        try {
            index.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public ResultSet search(String queryString) {
        String matchingModelName = "matching";
        String weightingModelName = "BM25";
        Map <String, Object> controls = new HashMap<>();
        Map <String, Object> properties = new HashMap<>();
        return this.search(queryString, matchingModelName, weightingModelName, controls, properties);
    }

    public ResultSet search(
            String queryString,
            String matchingModelName,
            String weightingModelName,
            Map<String, Object> controls,
            Map<String, Object> properties
    ){
        Manager queryingManager = new Manager(this.index);
        SearchRequest srq = queryingManager.newSearchRequestFromQuery(queryString);
        srq.addMatchingModel(matchingModelName, weightingModelName);

        srq.setControl("decorate", "on");
        int maxResults = Integer.parseInt((String) controls.getOrDefault("end", "1000"));
        srq.setNumberOfDocumentsAfterFiltering(maxResults);

        for (Map.Entry<String, Object> entry : controls.entrySet()) {
            srq.setControl(entry.getKey(), (String) entry.getValue());
        }

        HashSet <String> modifiedProperties = new HashSet<>();
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            ApplicationSetup.setProperty(entry.getKey(), (String) entry.getValue());
            modifiedProperties.add(entry.getKey());
        }

        // process the query
        queryingManager.runPreProcessing(srq);
        queryingManager.runMatching(srq);
        queryingManager.runPostProcessing(srq);
        queryingManager.runPostFilters(srq);

        // restore properties if changed
        restoreProperties(modifiedProperties);

        // return results
        return srq.getResultSet();

    }

    public String [] getDocNames (int [] docIds) throws IOException {
        String [] docNames = new String[docIds.length];
        for (int i = 0; i < docIds.length; i++) {
            docNames[i] = this.getDocNames(docIds[i]);
        }
        return docNames;
    }

    public String  getDocNames (int docId) throws IOException {
        return this.metaIndex.getAllItems(docId)[0];
    }

    public int [] getDocIds (String [] docNames) throws IOException {
        int [] docIds = new int[docNames.length];
        for (int i = 0; i < docIds.length; i++) {
            docIds[i] = this.getDocIds(docNames[i]);
        }
        return docIds;
    }

    public int getDocIds (String docName) throws IOException {
        return this.metaIndex.getDocument("docno", docName);
    }
}


public class TerrierServer {
    private TerrierCore core;

    public TerrierServer(String ipAddressString, int portNumber) {
        Spark.port(portNumber);
        Spark.ipAddress(ipAddressString);
        core = new TerrierCore();
    }

    private String makeSuccessResponse(Response response, Map <String, Object> body) {
        String bodyJson = (new JSONObject(body)).toString();
        response.body(bodyJson);
        response.status(200);
        return response.body();
    }

    private String makeErrorResponse(Response response, String errorType, String errorMessage, int errorCode){
        Map<String,String> bodyMap = new HashMap<>();
        bodyMap.put(errorType, errorMessage);
        String bodyJson = (new JSONObject(bodyMap)).toString();

        response.body(bodyJson);
        response.status(errorCode);
        return response.body();
    }

    private String search(Request request, Response response) {
        String body = request.body();
        Map <String, Object> parsedBody = new JSONObject(body).toMap();
//
        String query = (String) parsedBody.get("query");
        String matchingModelName = (String) parsedBody.getOrDefault("matchingModelName", "Matching");
        String weightingModelName = (String) parsedBody.getOrDefault("weightingModelName", "BM25");

        HashMap <String, Object> controls = new HashMap<>();
        if (parsedBody.containsKey("controls")) {
            controls = (HashMap<String, Object>) parsedBody.get("controls");
        }

        HashMap <String, Object> properties = new HashMap<>();
        if (parsedBody.containsKey("properties")) {
            properties = (HashMap<String, Object>) parsedBody.get("properties");
        }

        if (query == null){
            return makeErrorResponse(
                    response,
                    "RequestMalformedException",
                    "Query is missing from request",
                    501
            );
        }

        ResultSet results = null;
        try {
            results = core.search(query, matchingModelName, weightingModelName, controls, properties);
        } catch (Exception exec) {
            StringWriter errors = new StringWriter();
            exec.printStackTrace(new PrintWriter(errors));
            String stackTraceString = errors.toString();
            System.err.print(stackTraceString);
            return makeErrorResponse(response, exec.getClass().getName(), stackTraceString, 501);
        }
        int [] resultsDocIds = results.getDocids();
        double [] resultsDocScores = results.getScores();

        HashMap <String, Object> responseResults = new HashMap<>();
        responseResults.put("results", new ArrayList<HashMap>());

        for (int i=0; i < results.getResultSize(); i++){
            HashMap <String, Object> result = new HashMap<>();

            String docName = "";
            try {
                docName = core.getDocNames(resultsDocIds[i]);
            } catch (IOException exec) {
                StringWriter errors = new StringWriter();
                exec.printStackTrace(new PrintWriter(errors));
                return makeErrorResponse(response, exec.getClass().getName(), errors.toString(), 501);
            }

            result.put("_id", docName);
            result.put("_score", resultsDocScores[i]);
            ((ArrayList <HashMap>) responseResults.get("results")).add(result);
        }

        return makeSuccessResponse(response, responseResults);
    }

    private String stats(Request request, Response response) {
        CollectionStatistics stats = this.core.getCollectionStatistics();

        Map <String, Object> statsMap = new HashMap<>();
        statsMap.put("fields", stats.getNumberOfFields());
        statsMap.put("fields_tokens", stats.getFieldTokens());
        statsMap.put("fields_lengths", stats.getAverageFieldLengths());
        statsMap.put("documents", stats.getNumberOfDocuments());
        statsMap.put("tokens", stats.getNumberOfDocuments());
        statsMap.put("pointers", stats.getNumberOfPointers());
        statsMap.put("unique_terms", stats.getNumberOfUniqueTerms());
        statsMap.put("average_length", stats.getAverageDocumentLength());

        return makeSuccessResponse(response, statsMap);
    }

    private String endpointNotFound (Request request, Response response){
        return makeErrorResponse(
            response,
            "EndpointNotFound",
            "This endpoint does not exist",
            404
        );
    }

    private void defineRoutes() {
        // this route is used to search documents
        Spark.post("/_search", this::search);

        // this route returns stats about the index
        Spark.get("/_stats", this::stats);

        // this route handles all other endpoints
        // THIS ROUTE MUST BE THE LAST ONE
        Spark.get("*", this::endpointNotFound);

    }

    public static void main(String[] args) throws IOException, SAXException, ParserConfigurationException  {
        // raise the level of the logger
        Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.WARN);

        // parse input options, or use the defaults
        String ipAddressString = "localhost";
        int portNumber = 4567;
        if(args.length > 0) {
            ipAddressString = args[0];
        }
        if(args.length > 1) {
            portNumber = Integer.parseInt(args[1]);
        }

        // start the server, define routes
        TerrierServer ts = new TerrierServer(ipAddressString, portNumber);
        ts.defineRoutes();
        System.out.println(String.format("[info] Server running at %s:%d", ipAddressString, portNumber));

        // gracefully handle sigterm
        Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
            public void run() {
                spark.Spark.stop();
                ts.core.close();
                System.out.println("[info] Server stopped");
            }
        });
    }
}

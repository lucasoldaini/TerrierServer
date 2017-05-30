package net.soldaini.terrierServer;

import java.io.*;
import javax.xml.parsers.ParserConfigurationException;


import com.google.common.collect.ImmutableMap;
import com.sun.corba.se.spi.ior.ObjectKey;
import it.unimi.dsi.fastutil.Hash;
import org.json.JSONArray;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.json.JSONObject;
import static spark.Spark.*;

import org.terrier.matching.ResultSet;
import org.terrier.querying.Manager;
import org.terrier.structures.*;
import org.terrier.querying.SearchRequest;
import org.xml.sax.SAXException;
import spark.Request;
import spark.Response;
import sun.jvm.hotspot.debugger.cdbg.Sym;

import java.lang.String;
import java.util.*;


class TerrierCore {
    private Index index;
    private PostingIndex <Pointer> invertedIndex;
    private DocumentIndex documentIndex;
    private MetaIndex metaIndex;
    private Lexicon<String> lex;
    private org.terrier.terms.PorterStemmer stemmer;

    public TerrierCore(){
        index = Index.createIndex();
        invertedIndex = (PostingIndex<Pointer>) index.getInvertedIndex();
        documentIndex = index.getDocumentIndex();
        metaIndex = index.getMetaIndex();
        lex = index.getLexicon();
        stemmer = new org.terrier.terms.PorterStemmer();
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
        return this.search(queryString, matchingModelName, weightingModelName, controls);
    }

    public ResultSet search(
            String queryString,
            String matchingModelName,
            String weightingModelName,
            Map<String, Object> controls
    ){
        Manager queryingManager = new Manager(this.index);
        SearchRequest srq = queryingManager.newSearchRequestFromQuery(queryString);
        srq.addMatchingModel(matchingModelName, weightingModelName);

        srq.setControl("decorate", "on");
        int maxResults = (
            Integer.parseInt((String) controls.getOrDefault("end", "1000")) -
            Integer.parseInt((String) controls.getOrDefault("start", "0"))
        );
        srq.setNumberOfDocumentsAfterFiltering(maxResults);

        for (Map.Entry<String, Object> entry : controls.entrySet()) {
            srq.setControl(entry.getKey(), (String) entry.getValue());
        }

        // process the query
        queryingManager.runPreProcessing(srq);
        queryingManager.runMatching(srq);
        queryingManager.runPostProcessing(srq);
        queryingManager.runPostFilters(srq);

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
        port(portNumber);
        ipAddress(ipAddressString);
        core = new TerrierCore();
    }

    private String makeSuccessResponse(Response response, Map <String, Object> body) {
        String bodyJson = (new JSONObject(body)).toString();
        response.body(bodyJson);
        response.status(200);
        return response.body();
    }

    private String makeErrorResponse(Response response, String errorType, String errorMessage){
        Map<String,String> bodyMap = new HashMap<>();
        bodyMap.put(errorType, errorMessage);
        String bodyJson = (new JSONObject(bodyMap)).toString();

        response.body(bodyJson);
        response.status(501);
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

        if (query == null){
            return makeErrorResponse(
                    response,
                    "RequestMalformedException",
                    "Query is missing from request"
            );
        }

        ResultSet results = null;
        try {
            results = core.search(query, matchingModelName, weightingModelName, controls);
        } catch (Exception exec) {
            StringWriter errors = new StringWriter();
            exec.printStackTrace(new PrintWriter(errors));
            String stackTraceString = errors.toString();
            System.err.print(stackTraceString);
            return makeErrorResponse(response, exec.getClass().getName(), stackTraceString);
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
                return makeErrorResponse(response, exec.getClass().getName(), errors.toString());
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

    private void defineRoutes() {
        // this route is used to search documents
        post("/_search", this::search);

        // this route returns stats about the index
        get("/_stats", this::stats);
    }

    public static void main(String[] args) throws IOException, SAXException, ParserConfigurationException  {
        // raise the level of the logger
        Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.WARN);

        // parse input options, or use the defaults
        String ipAddressString = "localhost";
        int portNumber = 4567;
        if(args.length > 0) {
            ipAddressString = args[1];
        }
        if(args.length > 2) {
            portNumber = Integer.parseInt(args[2]);
        }

        // start the server, define routes
        TerrierServer ts = new TerrierServer(ipAddressString, portNumber);
        ts.defineRoutes();
        System.out.println("[info] Server started.");

        // gracefully handle sigterm
        Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
            public void run() {
                spark.Spark.stop();
                ts.core.close();
                System.out.println("[info] Server stopped.");
            }
        });
    }
}

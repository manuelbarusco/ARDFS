package search;

import analyze.CustomAnalyzer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.benchmark.quality.QualityQuery;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;
import parse.DatasetFields;
import utils.BoostWeights;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;

/**
 * This class will search for the best datasets that suit a given set of user queries
 * by searching in the reduced index
 *
 * @author Manuel Barusco
 * @version 1.0
 * @since 1.0
 */
public class ReducedDatasetSearcher {

    private IndexReader indexReader;            //Lucene object for index reading
    private IndexSearcher indexSearcher;        //Lucene object for index searching
    private Similarity similarity;              //Lucene Similarity object that must be used during the indexing phase
    private Analyzer analyzer;                  //Analyzer that mus be used during the indexing phase
    private String queryPath;                   //path to the queries file
    private QualityQuery[] queries;             //the queries that we must search for
    private String resultDirectoryPath;         //path to the directory where to save the resuls
    private int maxDatasetsRetrieved;           //max number of datasets to retrieve for every query

    /** Constructor
     *
     * @param indexPath path to the index directory
     * @param analyzer that must be used
     * @param similarity that must be used
     * @param resultsDirectoryPath path to directory where to save the runs results
     * @param maxDatasetsRetrieved max number of datasets to retrieve for every query
     * @throws IOException if there are problems when opening the queries file
     */
    public ReducedDatasetSearcher(String indexPath, Analyzer analyzer, Similarity similarity, String resultsDirectoryPath, String queryPath, int maxDatasetsRetrieved) throws IOException {
        //check for the indexPath
        if(indexPath == null || indexPath.isEmpty())
            throw new IllegalArgumentException("The index directory path cannot be null or empty");

        File indexDirectory = new File(indexPath);
        if(!indexDirectory.isDirectory() || !indexDirectory.exists())
            throw new IllegalArgumentException("The index directory specified does not exist");

        //check for analyzer and similarity
        if(analyzer == null || similarity == null)
            throw new IllegalArgumentException("The analyzer or the similarity cannot be null");

        this.analyzer = analyzer;
        this.similarity = similarity;

        try {
            indexReader = DirectoryReader.open(FSDirectory.open(indexDirectory.toPath()));
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot create the IndexReader for the directory: "+indexDirectory.getPath()+" Error: "+e);
        }

        indexSearcher = new IndexSearcher(indexReader);
        indexSearcher.setSimilarity(this.similarity);

        //check for the results directory path
        if(resultsDirectoryPath == null || resultsDirectoryPath.isEmpty())
            throw new IllegalArgumentException("The results directory path cannot be null or empty");

        File resultsDirectory = new File(resultsDirectoryPath);
        if(!resultsDirectory.isDirectory() || !resultsDirectory.exists())
            throw new IllegalArgumentException("The results directory specified does not exist");
        this.resultDirectoryPath = resultsDirectoryPath;

        //check for the query file path
        if(queryPath == null || queryPath.isEmpty())
            throw new IllegalArgumentException("The query path cannot be null or empty");

        File queryFile = new File(queryPath);
        if(!queryFile.isFile() || !queryFile.exists())
            throw new IllegalArgumentException("The query path specified does not exist or is not a file");

        //read the queries
        queries = new QueriesReader().readQueries(queryPath);

        this.maxDatasetsRetrieved = maxDatasetsRetrieved;
    }

    /**
     * This method will search only on the dataset meta-data fields
     * @param runID id of the run
     * @param queryWeights boost weights for the various query fields if we want to use boosting
     * @throws ParseException if there are problems during the query parsing
     * @throws IOException if the index searcher has some problems during the search in the index
     */
    public void searchInMetaData(String runID, HashMap<String, Float> queryWeights) throws ParseException, IOException {

        PrintWriter writer = new PrintWriter(resultDirectoryPath+"/"+runID+".txt");

        //specify the dataset fields where to search
        String[] fields = {DatasetFields.TITLE, DatasetFields.DESCRIPTION, DatasetFields.AUTHOR, DatasetFields.TAGS};

        for (QualityQuery q:  queries) {
            System.out.println("Searching for query: "+q.getQueryID());
            String queryID = q.getQueryID();

            Query query;

            //check for boosting parameters
            if (queryWeights != null)
                query = CustomQueryBuilder.buildBoostedQuery(q.getValue(QueryFields.TEXT), analyzer, queryWeights);
            else
                query = CustomQueryBuilder.buildBooleanQuery(fields, analyzer, q.getValue(QueryFields.TEXT));

            ScoreDoc[] docs = indexSearcher.search(query, maxDatasetsRetrieved).scoreDocs;
            writeResults(writer, runID, queryID, docs);
        }

        writer.close();
    }

    /**
     * This method will search only on the dataset content
     * @param runID id of the run
     * @param queryWeights boost weights for the various query fields if we want to use boosting
     * @throws ParseException if there are problems during the query parsing
     * @throws IOException if the index searcher has some problems during the search in the index
     */
    public void searchInContent(String runID, HashMap<String, Float> queryWeights) throws ParseException, IOException {

        //specify the dataset fields where to search, here we have only two fields
        String[] fields = {DatasetFields.ENTITIES, DatasetFields.LITERALS};

        PrintWriter writer = new PrintWriter(resultDirectoryPath+"/"+runID+".txt");

        for (QualityQuery q:  queries) {
            System.out.println("Searching for query: "+q.getQueryID());
            String queryID = q.getQueryID();

            Query query;

            //check for boosting parameters
            if (queryWeights != null)
                query = CustomQueryBuilder.buildBoostedQuery(q.getValue(QueryFields.TEXT), analyzer, queryWeights);
            else
                query = CustomQueryBuilder.buildBooleanQuery(fields, analyzer, q.getValue(QueryFields.TEXT));

            ScoreDoc[] docs = indexSearcher.search(query, maxDatasetsRetrieved).scoreDocs;
            writeResults(writer, runID, queryID, docs);
        }

        writer.close();
    }

    /**
     * This method will search only on all the dataset info (content and metadata)
     * This method can be used with and without boosting by passing or not the queryWeights
     * param.
     *
     * @param runID id of the run
     * @param queryWeights boost weights for the various query fields if we want to use boosting
     * @throws ParseException if there are problems during the query parsing
     * @throws IOException if the index searcher has some problems during the search in the index
     */
    public void searchInAllInfo(String runID, HashMap<String, Float> queryWeights) throws ParseException, IOException {

        PrintWriter writer = new PrintWriter(resultDirectoryPath+"/"+runID+".txt");

        //specify the dataset fields where to search
        String[] fields = {DatasetFields.TITLE, DatasetFields.DESCRIPTION, DatasetFields.AUTHOR, DatasetFields.TAGS, DatasetFields.ENTITIES, DatasetFields.LITERALS};

        for (QualityQuery q:  queries) {
            System.out.println("Searching for query: "+q.getQueryID());
            String queryID = q.getQueryID();

            Query query;

            //check for boosting parameters
            if (queryWeights != null)
                query = CustomQueryBuilder.buildBoostedQuery(q.getValue(QueryFields.TEXT), analyzer, queryWeights);
            else
                query = CustomQueryBuilder.buildBooleanQuery(fields, analyzer, q.getValue(QueryFields.TEXT));

            ScoreDoc[] docs = indexSearcher.search(query, maxDatasetsRetrieved).scoreDocs;
            writeResults(writer, runID, queryID, docs);
        }

        writer.close();
    }

    /**
     * This method will search for a single query and will print all the output rak info
     * The query will be searched in a [d], [m] and [m+d] configuration
     *
     * @param query_text text of the query
     * @param queryWeightsData boost weights for search in data fields
     * @param queryWeightsMetadata boost weights for search in metadata fields
     * @param queryWeightsAllFields boost weights for search in all fields
     */
    public void searchQuery(String query_text, HashMap<String, Float> queryWeightsData, HashMap<String, Float> queryWeightsMetadata, HashMap<String, Float> queryWeightsAllFields) throws ParseException, IOException {
        String[] data_fields = {DatasetFields.ENTITIES, DatasetFields.LITERALS};
        String[] all_fields = {DatasetFields.TITLE, DatasetFields.DESCRIPTION, DatasetFields.AUTHOR, DatasetFields.TAGS, DatasetFields.ENTITIES, DatasetFields.LITERALS};
        String[] metadata_fields = {DatasetFields.TITLE, DatasetFields.DESCRIPTION, DatasetFields.AUTHOR, DatasetFields.TAGS};

        Query queryData = CustomQueryBuilder.buildBoostedQuery(query_text, analyzer, queryWeightsData);
        Query queryAllFields = CustomQueryBuilder.buildBoostedQuery(query_text, analyzer, queryWeightsAllFields);
        Query queryMetadataFields = CustomQueryBuilder.buildBoostedQuery(query_text, analyzer, queryWeightsMetadata);

        ScoreDoc[] docsData = indexSearcher.search(queryData, 50). scoreDocs;
        ScoreDoc[] docsAll = indexSearcher.search(queryAllFields, 20).scoreDocs;
        ScoreDoc[] docsMetadata = indexSearcher.search(queryMetadataFields, 20).scoreDocs;

        System.out.println("------------    DATA     ------------");
        printResults(docsData);
        System.out.println("------------  METADATA   ------------");
        printResults(docsMetadata);
        System.out.println("------------    ALL       ------------");
        printResults(docsAll);

    }


    /**
     * This method will print the output results
     * @param docs array of ScoreDoc document retrieved
     */
    public void printResults(ScoreDoc[] docs) throws IOException {
        for(int i=0; i<docs.length; i++){
            String docID = indexReader.document(docs[i].doc, Collections.singleton(DatasetFields.ID)).get(DatasetFields.ID);
            System.out.printf(Locale.ENGLISH, "%s\t%d\t%.6f\t%n", docID, i, docs[i].score);
        }
    }

    /**
     * This method will create the run output file
     * @param writer PrintWriter object pointing to the run results file
     * @param runID string with the run identifier
     * @param queryID string the query identifier
     * @param docs array of ScoreDoc document retrieved
     * @throws IOException in case there are problems during the writing of the results
     */
    public void writeResults(PrintWriter writer, String runID, String queryID, ScoreDoc[] docs) throws IOException {
        HashSet<String> docsIds = new HashSet<>();
        for(int i=0; i<docs.length; i++){
            String docID = indexReader.document(docs[i].doc, Collections.singleton(DatasetFields.ID)).get(DatasetFields.ID);
            if (!docsIds.contains(docID)){
                writer.printf(Locale.ENGLISH, "%s\tQ0\t%s\t%d\t%.6f\t%s%n", queryID, docID, i, docs[i].score, runID);
                writer.flush();
                docsIds.add(docID);
            }
        }
    }

    /**
     * ONLY FOR DEBUG PURPOSES
     */
    public static void main(String[] args) throws IOException, ParseException {

        String indexPath = "/media/manuel/Tesi/Index/Index_HRv2_Stop_only_clean";
        String resultPath = "/home/manuel/Tesi/ACORDAR/Run/ARDFS";
        String queryPath = "/home/manuel/Tesi/ACORDAR/Data/all_queries.txt";
        Analyzer a = CustomAnalyzer.getStopwordsAnalyzer();

        Similarity s = new BM25Similarity();
        ReducedDatasetSearcher searcher = new ReducedDatasetSearcher(indexPath,a,s,resultPath, queryPath, 10);
        searcher.searchQuery("Debt Rescheduling", BoostWeights.BM25DataBoostWeights, BoostWeights.BM25MetadataBoostWeights, BoostWeights.BM25BoostWeights );

        /*

        // ---------- LMD ------------ //
        Similarity s = new LMDirichletSimilarity();
        ReducedDatasetSearcher searcher = new ReducedDatasetSearcher(indexPath,a,s,resultPath, queryPath, 10);
        searcher.searchInMetaData("LMD[m]", null);
        searcher.searchInContent("LMD[d]", null);
        searcher.searchInAllInfo("LMD[m+d]", null);

        // ---------- BM25 ------------ //
        s = new BM25Similarity();
        searcher = new ReducedDatasetSearcher(indexPath,a,s,resultPath, queryPath, 10);
        searcher.searchInMetaData("BM25[m]", null);
        searcher.searchInContent("BM25[d]", null);
        searcher.searchInAllInfo("BM25[m+d]", null);

        // ---------- TFIDF  ------------ //
        s = new ClassicSimilarity();
        searcher = new ReducedDatasetSearcher(indexPath,a,s,resultPath, queryPath, 10);
        searcher.searchInMetaData("TFIDF[m]", null);
        searcher.searchInContent("TFIDF[d]", null);
        searcher.searchInAllInfo("TFIDF[m+d]", null);


         */
    }


}

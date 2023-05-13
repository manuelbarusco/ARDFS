package search;

import index.DatasetIndexer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.benchmark.quality.QualityQuery;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;
import parse.DatasetFields;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Locale;

/**
 * This class will search for the best datasets that suit a given set of user queries
 *
 * @author Manuel Barusco
 * @version 1.0
 * @since 1.0
 */
public class DatasetSearcher {

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
    public DatasetSearcher(String indexPath, Analyzer analyzer, Similarity similarity, String resultsDirectoryPath, String queryPath, int maxDatasetsRetrieved) throws IOException {
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
     * @throws ParseException if there are problems during the query parsing
     * @throws IOException if the index searcher has some problems during the search in the index
     */
    public void searchInMetaData(String runID) throws ParseException, IOException {

        PrintWriter writer = new PrintWriter(resultDirectoryPath+"/"+runID+".txt");

        //specify the dataset fields where to search
        String[] fields = {DatasetFields.TITLE, DatasetFields.DESCRIPTION, DatasetFields.AUTHOR, DatasetFields.TAGS};

        for (QualityQuery q:  queries) {
            System.out.println("Searching for query: "+q.getQueryID());
            String queryID = q.getQueryID();
            Query query = CustomQueryBuilder.buildBooleanQuery(fields, analyzer, q.getValue(QueryFields.TEXT));
            ScoreDoc[] docs = indexSearcher.search(query, maxDatasetsRetrieved).scoreDocs;
            System.out.println(docs.length);
            writeResults(writer, runID, queryID, docs);
        }

        writer.close();
    }

    /**
     * This method will search only on the dataset content
     * @param runID id of the run
     * @throws ParseException if there are problems during the query parsing
     * @throws IOException if the index searcher has some problems during the search in the index
     */
    public void searchInContent(String runID) throws ParseException, IOException {

        //specify the dataset fields where to search
        String[] fields = {DatasetFields.CLASSES, DatasetFields.ENTITIES, DatasetFields.PROPERTIES, DatasetFields.LITERALS};

        PrintWriter writer = new PrintWriter(resultDirectoryPath+"/"+runID+".txt");

        for (QualityQuery q:  queries) {
            System.out.println("Searching for query: "+q.getQueryID());
            String queryID = q.getQueryID();
            Query query = CustomQueryBuilder.buildBooleanQuery(fields, analyzer, q.getValue(QueryFields.TEXT));
            ScoreDoc[] docs = indexSearcher.search(query, maxDatasetsRetrieved).scoreDocs;
            writeResults(writer, runID, queryID, docs);
            System.out.println(docs.length);
        }

        writer.close();
    }

    /**
     * This method will search only on all the dataset info (content and metadata)
     * @param runID id of the run
     * @throws ParseException if there are problems during the query parsing
     * @throws IOException if the index searcher has some problems during the search in the index
     */
    public void searchInAllInfo(String runID) throws ParseException, IOException {

        PrintWriter writer = new PrintWriter(resultDirectoryPath+"/"+runID+".txt");

        //specify the dataset fields where to search
        String[] fields = {DatasetFields.TITLE, DatasetFields.DESCRIPTION, DatasetFields.AUTHOR, DatasetFields.TAGS, DatasetFields.CLASSES, DatasetFields.ENTITIES, DatasetFields.PROPERTIES, DatasetFields.LITERALS};

        for (QualityQuery q:  queries) {
            String queryID = q.getQueryID();
            Query query = CustomQueryBuilder.buildBooleanQuery(fields, analyzer, q.getValue(QueryFields.TEXT));
            ScoreDoc[] docs = indexSearcher.search(query, maxDatasetsRetrieved).scoreDocs;
            writeResults(writer, runID, queryID, docs);
        }

        writer.close();
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
        for(int i=0; i<docs.length; i++){
            String docID = indexReader.document(docs[i].doc, Collections.singleton(DatasetFields.ID)).get(DatasetFields.ID);
            writer.printf(Locale.ENGLISH, "%s\tQ0\t%s\t%d\t%.6f\t%s%n", queryID, docID, i, docs[i].score, runID);
            writer.flush();
        }
    }

    /**
     * ONLY FOR DEBUG PURPOSES
     */
    public static void main(String[] args) throws IOException, ParseException {

        String indexPath = "/media/manuel/Tesi/IndexJENA_RDFLib";
        String resultPath = "/home/manuel/Tesi/ACORDAR/Run/EDS";
        String queryPath = "/home/manuel/Tesi/ACORDAR/Data/all_queries.txt";

        Analyzer a = new StandardAnalyzer();
        Similarity s = new BM25Similarity();

        DatasetSearcher searcher = new DatasetSearcher(indexPath,a,s,resultPath, queryPath, 100);
        searcher.searchInMetaData("BM25[m]");
        searcher.searchInContent("BM25[c]");
        searcher.searchInAllInfo("BM25[m+c]");

    }


}

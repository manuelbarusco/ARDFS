package search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.benchmark.quality.QualityQuery;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;
import parse.DatasetFields;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    public DatasetSearcher(String indexPath, Analyzer analyzer, Similarity similarity, String resultsDirectoryPath, int maxDatasetsRetrieved) throws IOException {
        //check for the indexPath
        if(indexPath.isEmpty() || indexPath == null)
            throw new IllegalArgumentException("The index directory path cannot be null or empty");

        File indexDirectory = new File(indexPath);
        if(!indexDirectory.isDirectory() || !indexDirectory.exists())
            throw new IllegalArgumentException("The index directory specified does not exist");

        //check for analyzer and similarity
        if(analyzer == null || similarity == null)
            throw new IllegalArgumentException("The analyzer or the similarity cannot be null");

        this.analyzer = analyzer;
        this.similarity = similarity;

        final Path indexDir = Paths.get(indexPath);
        try {
            indexReader = DirectoryReader.open(FSDirectory.open(indexDir));
        } catch (IOException e) {
            throw new IllegalArgumentException(String.format("Unable to create the index reader for directory %s: %s.",
                    indexDir.toAbsolutePath(), e.getMessage()), e);
        }

        indexSearcher = new IndexSearcher(indexReader);
        indexSearcher.setSimilarity(similarity);

        //check for the results directory path
        if(resultsDirectoryPath.isEmpty() || resultsDirectoryPath == null)
            throw new IllegalArgumentException("The results directory path cannot be null or empty");

        File resultsDirectory = new File(resultsDirectoryPath);
        if(!indexDirectory.isDirectory() || !indexDirectory.exists())
            throw new IllegalArgumentException("The results directory specified does not exist");

        //read the queries
        queries = new QueriesReader().readQueries(queryPath);
    }

    /**
     * This method will search only on the dataset meta-data
     * @param runID id of the run
     * @throws ParseException if there are problems during the query parsing
     * @throws IOException if the index searcher has some problems during the search in the index
     */
    public void searchInMetaData(String runID) throws ParseException, IOException {

        //specify the dataset fields where to search
        String[] fields = {DatasetFields.TITLE, DatasetFields.DESCRIPTION, DatasetFields.AUTHOR, DatasetFields.TAGS};

        for (QualityQuery q:  queries) {
            String queryID = q.getQueryID();
            Query query = CustomQueryBuilder.buildBooleanQuery(fields, analyzer, q.getValue(QueryFields.TEXT));
            ScoreDoc[] docs = indexSearcher.search(query, maxDatasetsRetrieved).scoreDocs;
            writeResults(runID, queryID, docs);
        }
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

        for (QualityQuery q:  queries) {
            String queryID = q.getQueryID();
            Query query = CustomQueryBuilder.buildBooleanQuery(fields, analyzer, q.getValue(QueryFields.TEXT));
            ScoreDoc[] docs = indexSearcher.search(query, maxDatasetsRetrieved).scoreDocs;
            writeResults(runID, queryID, docs);
        }
    }

    /**
     * This method will search only on all the dataset info (content and metadata)
     * @param runID id of the run
     * @throws ParseException if there are problems during the query parsing
     * @throws IOException if the index searcher has some problems during the search in the index
     */
    public void searchInAllInfo(String runID) throws ParseException, IOException {

        //specify the dataset fields where to search
        String[] fields = {DatasetFields.TITLE, DatasetFields.DESCRIPTION, DatasetFields.AUTHOR, DatasetFields.TAGS, DatasetFields.CLASSES, DatasetFields.ENTITIES, DatasetFields.PROPERTIES, DatasetFields.LITERALS};

        for (QualityQuery q:  queries) {
            String queryID = q.getQueryID();
            Query query = CustomQueryBuilder.buildBooleanQuery(fields, analyzer, q.getValue(QueryFields.TEXT));
            ScoreDoc[] docs = indexSearcher.search(query, maxDatasetsRetrieved).scoreDocs;
            writeResults(runID, queryID, docs);
        }
    }

    /**
     * This method will create the run output file
     * @param runID string with the run identifier
     * @param queryID string the query identifier
     * @param docs array of ScoreDoc document retrieved
     * @throws IOException in case there are problems during the writing of the results
     */
    public void writeResults(String runID, String queryID, ScoreDoc[] docs) throws FileNotFoundException {
        PrintWriter writer = new PrintWriter(resultDirectoryPath+"/"+queryID+".txt");
        for(int i=0; i<docs.length; i++){
            writer.printf(Locale.ENGLISH, "%s\tQ0\t%s\t%d\t%.6f\t%s%n", queryID, docs[i].doc, i, docs[i].score, runID);
        }
        writer.flush();
        writer.close();
    }


}

package search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;
import parse.DatasetReader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

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

    private Query[] queries;

    /** Constructor
     *
     * @param indexPath path to the index directory
     * @param analyzer that must be used
     * @param similarity that must be used
     */
    public DatasetSearcher(String indexPath, Analyzer analyzer, Similarity similarity){
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

        //read the queries
    }
}

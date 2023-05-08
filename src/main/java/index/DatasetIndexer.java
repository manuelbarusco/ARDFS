package index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.similarities.Similarity;
import parse.DatasetFields;
import parse.DatasetReader;
import utils.DatasetContent;
import utils.DatasetMetaData;

import java.io.File;
import java.io.IOException;

/**
 * This class will wrap the indexing phase of EDS
 *
 * @author Manuel Barusco
 * @version 1.0
 * @since 1.0
 */

public class DatasetIndexer {

    private String indexPath;           //Path to the directory where to store the index
    private IndexWriter writer;         //Lucene object for creating an index
    private Similarity similarity;      //Lucene Similarity object that must be used during the indexing phase
    private Analyzer analyzer;          //Analyzer that mus be used during the indexing phase

    /**
     * Constructor: this method will create the object and set the IndexWriter
     * @param indexPath string with the path to the directory where to store the index
     * @param similarity similarity that must be used during the indexing
     * @param analyzer analyzer that must be used during the indexing phase
     */
    public DatasetIndexer(String indexPath, Similarity similarity, Analyzer analyzer){
        //check for the indexPath
        if(indexPath.isEmpty() || indexPath == null)
            throw new IllegalArgumentException("The index directory path cannot be null or empty");

        File indexDirectory = new File(indexPath);
        if(!indexDirectory.isDirectory() || !indexDirectory.exists())
            throw new IllegalArgumentException("The index directory specified does not exist");

        //setup the IndexWriter object
        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setSimilarity(similarity);
        iwc.setRAMBufferSizeMB(2048.0);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setCommitOnClose(true);
        iwc.setUseCompoundFile(true);
    }

    /**
     * This method will index all the datasets contained in the datasetsFolderPath directory
     * @param datasetsDirectoryPath path to the directory where all the datasets are stored
     */
    public void indexDatasets(String datasetsDirectoryPath) throws IOException {
        //check for the datasetsDirectoryPath
        if(datasetsDirectoryPath.isEmpty() || datasetsDirectoryPath == null)
            throw new IllegalArgumentException("The datasets directory path cannot be null or empty");

        File datasetsDirectory = new File(datasetsDirectoryPath);
        if(!datasetsDirectory.isDirectory() || !datasetsDirectory.exists())
            throw new IllegalArgumentException("The datasets directory specified does not exist");

        File[] datasets = datasetsDirectory.listFiles();

        int datasetCount = 0;

        for(File dataset: datasets){

            DatasetReader reader = new DatasetReader(dataset.getAbsolutePath());
            DatasetMetaData metaData = reader.getMetaData();
            DatasetContent content = reader.getContent();

            index(metaData, content);
            datasetCount++;

            if(datasetCount % 50 == 0)
                writer.commit();
        }

        writer.close();
    }

    /**
     * This method will index a single dataset
     *
     * @param metaData a DatasetMetaData object with all the dataset meta info
     * @param content a DatasetContent object with all the dataset content
     * @throws IOException if there are problems during the index writing of the dataset
     */
    public void index(DatasetMetaData metaData, DatasetContent content) throws IOException {
        Document dataset = new Document();

        dataset.add(new DatasetField(DatasetFields.ID, metaData.dataset_id));
        dataset.add(new DatasetField(DatasetFields.TITLE, metaData.title));
        dataset.add(new DatasetField(DatasetFields.DESCRIPTION, metaData.description));
        dataset.add(new DatasetField(DatasetFields.AUTHOR, metaData.author));

        //split the tags
        String stringTags = metaData.tags;
        String[] tags = stringTags.split(":");

        for(String tag: tags)
            dataset.add(new DatasetField(DatasetFields.TAGS, tag));

        for(String entity: content.entities)
            dataset.add(new DatasetField(DatasetFields.ENTITIES, entity));

        for(String dClass: content.classes)
            dataset.add(new DatasetField(DatasetFields.CLASSES, dClass));

        for(String literal: content.literals)
            dataset.add(new DatasetField(DatasetFields.LITERALS, literal));

        for(String property: content.properties)
            dataset.add(new DatasetField(DatasetFields.PROPERTIES, property));

        writer.addDocument(dataset);
    }



}

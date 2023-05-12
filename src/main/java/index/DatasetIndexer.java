package index;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;
import parse.DatasetFields;
import parse.DatasetReader;
import utils.DatasetContent;
import utils.DatasetMetaData;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * This class will wrap the indexing phase of EDS
 *
 * @author Manuel Barusco
 * @version 1.0
 * @since 1.0
 */

public class DatasetIndexer {

    private File indexDirectory;                        //File object to the index directory
    private IndexWriter writer;                         //Lucene object for creating an index
    private IndexWriterConfig iwc;                      //IndexWriterConfig of the IndexWriter $writer wrapped inside
    private Similarity similarity;                      //Lucene Similarity object that must be used during the indexing phase
    private Analyzer analyzer;                          //Analyzer that mus be used during the indexing phase
    private static final int RAMBUFFER_SIZE = 2048 ;    //RAMBuffer size limit for the index writer

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

        indexDirectory = new File(indexPath);
        if(!indexDirectory.isDirectory() || !indexDirectory.exists())
            throw new IllegalArgumentException("The index directory specified does not exist");

        //set up the IndexWriterConfig object
        iwc = new IndexWriterConfig();
        iwc.setSimilarity(similarity);
        iwc.setRAMBufferSizeMB(RAMBUFFER_SIZE);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setCommitOnClose(true);
        iwc.setUseCompoundFile(true);

    }

    /**
     * This method will index all the datasets contained in the datasetsFolderPath directory
     * @param datasetsDirectoryPath path to the directory where all the datasets are stored
     */
    public void indexDatasets(String datasetsDirectoryPath) throws IOException {

        //intialize the IndexWriter Object
        try {
            writer = new IndexWriter(FSDirectory.open(indexDirectory.toPath()), iwc);
        } catch (IOException e) {
            throw new IllegalArgumentException("Unable to create the index files in the directory: "+indexDirectory.getPath()+" error: "+e);
        }

        //check for the datasetsDirectoryPath
        if(datasetsDirectoryPath.isEmpty() || datasetsDirectoryPath == null)
            throw new IllegalArgumentException("The datasets directory path cannot be null or empty");

        File datasetsDirectory = new File(datasetsDirectoryPath);
        if(!datasetsDirectory.isDirectory() || !datasetsDirectory.exists())
            throw new IllegalArgumentException("The datasets directory specified does not exist");

        File[] datasets = datasetsDirectory.listFiles();

        int datasetCount = 0;

        for(File dataset: datasets){

            if (dataset.isDirectory()) {

                //check if the dataset was already indexed

                if (!isIndexed(dataset)){

                    System.out.println("Indexing dataset: " + dataset.getName());

                    DatasetReader reader = new DatasetReader(dataset.getAbsolutePath());
                    DatasetMetaData metaData = reader.getMetaData();
                    DatasetContent contentJena = reader.getContentJena();
                    DatasetContent contentRDFLib = reader.getContentRDFLib();

                    indexDataset(metaData, contentJena, contentRDFLib);
                    datasetCount++;

                    //TODO: tune the parameter
                    if (datasetCount % 50 == 0)
                        writer.commit();

                }

            }
        }

        writer.close();
    }

    /**
     * This method tell if a given dataset is already indexed
     *
     * @param dataset File object that points to the dataset directory
     * @return True if the dataset is already indexed else False
     * @throws IOException if there are problems with the dataset_metadata.json file of the dataset
     */
    private boolean isIndexed(File dataset) throws IOException {
        FileReader metadataReader = new FileReader(dataset.getPath());
        JsonElement metadataJson = JsonParser.parseReader(metadataReader);
        JsonObject metadata = metadataJson.getAsJsonObject();
        metadataReader.close();
        if(!metadata.has("indexed"))
            return false;
        return metadata.get("indexed").getAsBoolean();
    }

    /**
     * This method will index a single dataset
     *
     * @param metaData a DatasetMetaData object with all the dataset meta info
     * @param contentJena a DatasetContent object with all the dataset content extracted from JENA
     * @param contentRDFLib a DatasetContent object with all the dataset content extracted from RDFLib
     * @throws IOException if there are problems during the index writing of the dataset
     */
    private void indexDataset(DatasetMetaData metaData, DatasetContent contentJena, DatasetContent contentRDFLib) throws IOException {
        Document dataset = new Document();

        dataset.add(new DatasetIdField(DatasetFields.ID, metaData.dataset_id));
        dataset.add(new DatasetField(DatasetFields.TITLE, metaData.title));
        dataset.add(new DatasetField(DatasetFields.DESCRIPTION, metaData.description));

        if (metaData.author!=null)
            dataset.add(new DatasetField(DatasetFields.AUTHOR, metaData.author));

        //split the tags
        String stringTags = metaData.tags;
        String[] tags = stringTags.split(":");

        for(String tag: tags)
            dataset.add(new DatasetField(DatasetFields.TAGS, tag));

        for(String entity: contentJena.entities)
            dataset.add(new DatasetField(DatasetFields.ENTITIES, entity));

        for(String dClass: contentJena.classes)
            dataset.add(new DatasetField(DatasetFields.CLASSES, dClass));

        for(String literal: contentJena.literals)
            dataset.add(new DatasetField(DatasetFields.LITERALS, literal));

        for(String property: contentJena.properties)
            dataset.add(new DatasetField(DatasetFields.PROPERTIES, property));

        //release memory
        contentJena = null;
        System.gc();

        dataset = new Document();
        dataset.add(new DatasetField(DatasetFields.ID, metaData.dataset_id));

        if(contentRDFLib!=null){
            for(String entity: contentRDFLib.entities)
                dataset.add(new DatasetField(DatasetFields.ENTITIES, entity));

            for(String dClass: contentRDFLib.classes)
                dataset.add(new DatasetField(DatasetFields.CLASSES, dClass));

            for(String literal: contentRDFLib.literals)
                dataset.add(new DatasetField(DatasetFields.LITERALS, literal));

            for(String property: contentRDFLib.properties)
                dataset.add(new DatasetField(DatasetFields.PROPERTIES, property));
        }

        //System.out.println((Runtime.getRuntime().totalMemory() / (1024*1024)) - (Runtime.getRuntime().freeMemory() / (1024*1024) ));

        writer.addDocument(dataset);
    }

    /**
     * ONLY FOR DEBUG PURPOSES
     */
    public static void main(String[] args) throws IOException {

        String indexPath = "/media/manuel/Tesi/Index";
        String datasetsDirectoryPath = "/home/manuel/Tesi/ACORDAR/Datasets";

        Analyzer a = new StandardAnalyzer();
        Similarity s = new BM25Similarity();

        DatasetIndexer indexer = new DatasetIndexer(indexPath, s, a);
        indexer.indexDatasets(datasetsDirectoryPath);

    }

}

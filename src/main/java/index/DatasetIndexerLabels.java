package index;

import analyze.CustomAnalyzer;
import com.google.gson.*;
import org.apache.lucene.analysis.Analyzer;
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
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Scanner;

/**
 * This class will execute the indexing phase of EDS
 *
 * @author Manuel Barusco
 * @version 1.0
 * @since 1.0
 */

public class DatasetIndexerLabels {

    private File indexDirectory;                        //File object to the index directory
    private IndexWriter writer;                         //Lucene object for creating an index
    private IndexWriterConfig iwc;                      //IndexWriterConfig of the IndexWriter $writer wrapped inside
    private static final int RAMBUFFER_SIZE = 2048 ;    //RAMBuffer size limit for the index writer
    private final boolean resume;                       //indicates if the indexer is in resume mode or not
    private final boolean no_empty_datasets;            //indicates if we have to skip the empty datasets
    private JsonObject datasetMetadata;                 //metadata of the dataset

    /**
     * Constructor: this method will create the object and set the IndexWriter
     * @param indexPath string with the path to the directory where to store the index
     * @param similarity similarity that must be used during the indexing
     * @param analyzer analyzer that must be used during the indexing phase
     * @param resume boolean that indicates if we have to resume an indexing process
     * @param no_empty_datasets boolean that indicates if we want to ignore the empty datasets
     */
    public DatasetIndexerLabels(String indexPath, Similarity similarity, Analyzer analyzer, boolean resume, boolean no_empty_datasets){
        //check for the indexPath
        if(indexPath.isEmpty() || indexPath == null)
            throw new IllegalArgumentException("The index directory path cannot be null or empty");

        indexDirectory = new File(indexPath);
        if(!indexDirectory.isDirectory() || !indexDirectory.exists())
            throw new IllegalArgumentException("The index directory specified does not exist");

        //set up the IndexWriterConfig object
        iwc = new IndexWriterConfig(analyzer);
        iwc.setSimilarity(similarity);
        iwc.setRAMBufferSizeMB(RAMBUFFER_SIZE);
        if(resume)
            iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
        else
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setCommitOnClose(true);
        iwc.setUseCompoundFile(true);

        this.resume = resume;
        this.no_empty_datasets = no_empty_datasets;

    }

    /**
     * This method returns true if a given dataset is considered big (at least one of the input files is bigger
     * than 1 GB)
     *
     * @param entities File object that point to the list of entities in the dataset extracted with LightRDF
     * @param properties File object that point to the list of properties in the dataset extracted with LightRDF
     * @param literals File object that point to the list of literals in the dataset extracted with LightRDF
     * @param classes File object that point to the list of classes in the dataset extracted with LightRDF
     * @return True if the dataset is considered big else False
     */
    public boolean isBigDataset(File entities, File properties, File literals, File classes){
        return (entities.length() / Math.pow(1024, 2) > 1000) || (properties.length() / Math.pow(1024, 2) > 1000) || (literals.length() / Math.pow(1024, 2) > 1000) || (classes.length() / Math.pow(1024, 2) > 1000);
    }

    /**
     * This method read and load the dataset metadata in the indexer private field
     * @param dataset File object that point to the dataset
     * @throws IOException if there are problems with the opening/closing of the dataset_metadata.json file
     */
    private void readDatasetMetadata(File dataset) throws IOException {
        FileReader metadataReader = new FileReader(dataset.getPath()+"/dataset_metadata.json");
        JsonElement metadataJson = JsonParser.parseReader(metadataReader);
        datasetMetadata = metadataJson.getAsJsonObject();
        metadataReader.close();
    }

    /**
     * This method tell if a given dataset is already indexed by considering the dataset_metadata.json file
     * already read.
     * @throws NullPointerException if the dataset_metadata.json file is not read before the method invocation
     * @return true if the dataset was already indexed else false
     */
    private boolean isIndexed() throws NullPointerException {
        if(!datasetMetadata.has("indexed"))
            return false;
        return datasetMetadata.get("indexed").getAsBoolean();
    }

    /**
     * This method tell if a given dataset is empty or not
     * @throws NullPointerException if the dataset_metadata.json file is not read before the method invocation
     * @return true if the dataset is empty else false
     */
    private boolean isEmpty(){
        boolean empty = true;
        if(datasetMetadata.has("mined_files_jena"))
            empty = empty && (datasetMetadata.get("mined_files_jena").getAsJsonArray().size() == 0);

        if(datasetMetadata.has("mined_files_rdflib"))
            empty = empty && (datasetMetadata.get("mined_files_rdflib").getAsJsonArray().size() == 0);

        if(datasetMetadata.has("mined_files_lightrdf"))
            empty = empty && (datasetMetadata.get("mined_files_lightrdf").getAsJsonArray().size() == 0);

        return empty;
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
        int indexedDatasets = 0;

        HashSet<String> skipDatasets = new HashSet<>();
        skipDatasets.add("dataset-11580");

        FileWriter listWriter = new FileWriter("list.txt");

        for(File dataset: datasets){

            if (dataset.isDirectory() && !skipDatasets.contains(dataset.getName())) {

                //read the dataset metadata
                readDatasetMetadata(dataset);

                //check if the dataset is empty
                boolean empty = false;
                if(no_empty_datasets)
                    empty = isEmpty();

                //check if the dataset was already indexed
                boolean indexed = false;
                if (resume)
                    indexed = isIndexed();

                if (!indexed && !empty){

                    DatasetReader reader = new DatasetReader(dataset.getAbsolutePath());
                    DatasetMetaData metaData = reader.getMetaData();

                    //if the dataset is mined from RDFLib or Jena we recover the json file for the content
                    //else the two variables have null value
                    DatasetContent contentRDFLibHR = reader.getContentRDFLibHRClean();

                    try {
                        indexDataset(metaData, contentRDFLibHR);

                    } catch (OutOfMemoryError e ){
                        System.out.println("Out of Memory in dataset:"+dataset.getName());

                    }

                    //force to release memory
                    contentRDFLibHR = null;
                    System.gc();

                    //update the dataset_metadata.json file with the "indexed" field
                    updateMetadata(dataset);

                    if (indexedDatasets % 50 == 0)
                        writer.commit();

                    indexedDatasets += 1;

                    listWriter.write(dataset.getName()+"\n");
                    listWriter.flush();
                }

                if (datasetCount % 100 == 0)
                    System.out.println("Scanned: "+datasetCount+"   Indexed: "+indexedDatasets);

                datasetCount++;

            }
        }
        listWriter.close();
        writer.close();
    }

    /**
     * This method update the dataset_metadata.json file after the dataset indexing
     * @param dataset File object that points to the dataset directory
     * @throws IOException if there are problems with the dataset_metadata.json file of the dataset
     */
    private void updateMetadata(File dataset){
        datasetMetadata.addProperty("indexed", true);

        try{
            FileWriter datasetMetadataFile=new FileWriter(dataset.getPath()+"/dataset_metadata.json", StandardCharsets.UTF_8);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            gson.toJson(datasetMetadata, datasetMetadataFile);
            datasetMetadataFile.close();
        } catch (IOException e){
            System.out.println("Error while updating the dataset_metadata.json file: "+e);
        }
    }

    /**
     * This method will index a single dataset
     *
     * @param metaData a DatasetMetaData object with all the dataset meta info
     * @param contentRDFLibHR a DatasetContent object with all the dataset content extracted from RDFLib
     * @throws IOException if there are problems during the index writing of the dataset
     */
    private void indexDataset(DatasetMetaData metaData, DatasetContent contentRDFLibHR) throws IOException {

        Document dataset = new Document();

        dataset.add(new MetadataField(DatasetFields.ID, metaData.dataset_id));
        dataset.add(new MetadataField(DatasetFields.TITLE, metaData.title));

        if (metaData.description != null)
            dataset.add(new MetadataField(DatasetFields.DESCRIPTION, metaData.description));

        if (metaData.author!=null)
            dataset.add(new MetadataField(DatasetFields.AUTHOR, metaData.author));

        //split the tags
        String stringTags = metaData.tags;
        String[] tags = stringTags.split(":");

        for(String tag: tags)
            dataset.add(new MetadataField(DatasetFields.TAGS, tag));

        //add the content extracted by RDFLibHR

        if(contentRDFLibHR!=null){
            for(String entity: contentRDFLibHR.entities)
                dataset.add(new DataField(DatasetFields.ENTITIES, entity));

            for(String dClass: contentRDFLibHR.classes)
                dataset.add(new DataField(DatasetFields.CLASSES, dClass));

            for(String literal: contentRDFLibHR.literals)
                dataset.add(new DataField(DatasetFields.LITERALS, literal));

            for(String property: contentRDFLibHR.properties)
                dataset.add(new DataField(DatasetFields.PROPERTIES, property));
        }

        //release memory
        contentRDFLibHR = null;
        System.gc();

        //System.out.println((Runtime.getRuntime().totalMemory() / (1024*1024)) - (Runtime.getRuntime().freeMemory() / (1024*1024) ));

        try {
            writer.addDocument(dataset);
        } catch (OutOfMemoryError e){
            System.out.println("OutOfMemory in dataset: "+metaData.dataset_id);
        }
    }

    /**
     * ONLY FOR DEBUG PURPOSES
     */
    public static void main(String[] args) throws IOException {

        String indexPath = "/media/manuel/Tesi/Index/Labelsv2_Parsing_Clean";
        String datasetsDirectoryPath = "/media/manuel/Tesi/Datasets";
        //String datasetsDirectoryPath = "/home/manuel/Tesi/ACORDAR/Datasets";

        Analyzer a = CustomAnalyzer.getStopwordsAnalyzer();
        Similarity s = new BM25Similarity();

        boolean resume = false;
        boolean no_empty_dataset = false;
        DatasetIndexerLabels indexer = new DatasetIndexerLabels(indexPath, s, a, resume, no_empty_dataset);

        indexer.indexDatasets(datasetsDirectoryPath);

    }

}

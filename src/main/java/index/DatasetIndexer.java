package index;

import com.google.gson.*;

import java.io.*;
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
import java.nio.charset.StandardCharsets;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Scanner;

/**
 * This class will execute the indexing phase of EDS
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
    private boolean resume;                             //indicates if the indexere is in resume mode or not

    /**
     * Constructor: this method will create the object and set the IndexWriter
     * @param indexPath string with the path to the directory where to store the index
     * @param similarity similarity that must be used during the indexing
     * @param analyzer analyzer that must be used during the indexing phase
     * @param resume boolean that indicates if we have to resume an indexing process
     */
    public DatasetIndexer(String indexPath, Similarity similarity, Analyzer analyzer, boolean resume){
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
        if(resume)
            iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
        else
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setCommitOnClose(true);
        iwc.setUseCompoundFile(true);

        this.resume = resume;

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

        HashSet<String> skipDatasets = new HashSet<>();
        skipDatasets.add("dataset-11580");
        skipDatasets.add("dataset-15243");

        for(File dataset: datasets){

            if (dataset.isDirectory() && !skipDatasets.contains(dataset.getName())) {

                //check if the dataset was already indexed

                boolean indexed = false;
                if (resume)
                    indexed = isIndexed(dataset);

                if (!indexed){

                    DatasetReader reader = new DatasetReader(dataset.getAbsolutePath());
                    DatasetMetaData metaData = reader.getMetaData();

                    //if the dataset is mined from RDFLib or Jena we recover the json file for the content
                    //else the two variables have null value
                    DatasetContent contentJena = reader.getContentJena();
                    DatasetContent contentRDFLib = reader.getContentRDFLib();

                    //check if the dataset is mined from LightRDF
                    Scanner entitiesFile = null;
                    Scanner classesFile = null;
                    Scanner literalsFile = null;
                    Scanner propertiesFile = null;

                    File entities = new File(dataset.getPath()+"/entities_lightrdf.txt");
                    File classes = new File(dataset.getPath()+"/classes_lightrdf.txt");
                    File literals = new File(dataset.getPath()+"/literals_lightrdf.txt");
                    File properties = new File(dataset.getPath()+"/properties_lightrdf.txt");

                    if(entities.exists()){
                        entitiesFile = new Scanner(entities);
                        classesFile = new Scanner(classes);
                        propertiesFile = new Scanner(properties);
                        literalsFile = new Scanner(literals);
                    }

                    try {
                        indexDataset(metaData, contentJena, contentRDFLib, entitiesFile, classesFile, propertiesFile, literalsFile);

                    } catch (OutOfMemoryError e ){
                        System.out.println("Out of Memory in dataset:"+dataset.getName());

                    }

                    //force to release memory
                    contentRDFLib = null;
                    contentJena = null;
                    System.gc();

                    //update the dataset_metadata.json file with the "indexed" field
                    updateMetadata(dataset);

                    //TODO: tune the parameter
                    //if (datasetCount % 10 == 0)
                    writer.commit();

                }

                if (datasetCount % 100 == 0)
                    System.out.println("Indexed: "+datasetCount);

                datasetCount++;

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
        FileReader metadataReader = new FileReader(dataset.getPath()+"/dataset_metadata.json");
        JsonElement metadataJson = JsonParser.parseReader(metadataReader);
        JsonObject metadata = metadataJson.getAsJsonObject();
        metadataReader.close();
        if(!metadata.has("indexed"))
            return false;
        return metadata.get("indexed").getAsBoolean();
    }

    /**
     * This method update the dataset_metadata.json file after the dataset indexing
     * @param dataset File object that points to the dataset directory
     * @throws IOException if there are problems with the dataset_metadata.json file of the dataset
     */
    private void updateMetadata(File dataset){
        JsonElement json = null;
        try {
            Reader reader = new FileReader(dataset.getPath()+"/dataset_metadata.json", StandardCharsets.UTF_8);
            json = JsonParser.parseReader(reader);
            reader.close();
        } catch (IOException e) {
            System.out.println("Error while reading the dataset_metadata.json file: "+e);
        }

        //get the JsonObject to udpate and update
        JsonObject datasetMetadata = json.getAsJsonObject();
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
     * @param contentJena a DatasetContent object with all the dataset content extracted from JENA
     * @param contentRDFLib a DatasetContent object with all the dataset content extracted from RDFLib
     * @param entitiesFile file with the entities extracted from LightRDF
     * @param classesFile file with the classes extracted from LightRDF
     * @param propertiesFile file with the properties extracted from LightRDF
     * @param literalsFile file with the literals extracted from LightRDF
     * @throws IOException if there are problems during the index writing of the dataset
     */
    private void indexDataset(DatasetMetaData metaData, DatasetContent contentJena, DatasetContent contentRDFLib, Scanner entitiesFile, Scanner classesFile, Scanner propertiesFile, Scanner literalsFile) throws IOException {
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

        //add the content extracted by JENA

        if (contentJena != null) {
            for (String entity : contentJena.entities)
                dataset.add(new DataField(DatasetFields.ENTITIES, entity));

            for (String dClass : contentJena.classes)
                dataset.add(new DataField(DatasetFields.CLASSES, dClass));

            for (String literal : contentJena.literals)
                dataset.add(new DataField(DatasetFields.LITERALS, literal));

            for (String property : contentJena.properties)
                dataset.add(new DataField(DatasetFields.PROPERTIES, property));
        }

        //release memory
        contentJena = null;
        System.gc();

        //add the content extracted by RDFLib

        if(contentRDFLib!=null){
            for(String entity: contentRDFLib.entities)
                dataset.add(new DataField(DatasetFields.ENTITIES, entity));

            for(String dClass: contentRDFLib.classes)
                dataset.add(new DataField(DatasetFields.CLASSES, dClass));

            for(String literal: contentRDFLib.literals)
                dataset.add(new DataField(DatasetFields.LITERALS, literal));

            for(String property: contentRDFLib.properties)
                dataset.add(new DataField(DatasetFields.PROPERTIES, property));
        }

        //release memory
        contentRDFLib = null;
        System.gc();

        //check if there are elements from LightRDF

        if(entitiesFile != null){
            while(entitiesFile.hasNext()){
                dataset.add(new DataField(DatasetFields.ENTITIES, entitiesFile.nextLine().replace("\n", "")));
            }

            while(classesFile.hasNext()){
                dataset.add(new DataField(DatasetFields.CLASSES, classesFile.nextLine().replace("\n", "")));
            }

            while(literalsFile.hasNext()){
                dataset.add(new DataField(DatasetFields.LITERALS, literalsFile.nextLine().replace("\n", "")));
            }

            while(propertiesFile.hasNext()){
                dataset.add(new DataField(DatasetFields.PROPERTIES, propertiesFile.nextLine().replace("\n", "")));
            }
            entitiesFile.close();
            classesFile.close();
            literalsFile.close();
            propertiesFile.close();

        }

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

        String indexPath = "/media/manuel/Tesi/Index";
        String datasetsDirectoryPath = "/media/manuel/Tesi/Datasets";
        //String datasetsDirectoryPath = "/home/manuel/Tesi/ACORDAR/Datasets";

        Analyzer a = new StandardAnalyzer();
        Similarity s = new BM25Similarity();

        boolean resume = false;
        DatasetIndexer indexer = new DatasetIndexer(indexPath, s, a, resume);

        indexer.indexDatasets(datasetsDirectoryPath);

    }

}

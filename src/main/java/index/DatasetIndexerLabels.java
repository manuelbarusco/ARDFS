package index;

import analyze.CustomAnalyzer;
import com.google.gson.*;

import java.io.*;
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
import java.nio.charset.StandardCharsets;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Scanner;

/**
 * This class will execute the indexing phase by using the
 * data extracted by the rdflibExtractorHR.py
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
    private boolean resume;                             //indicates if the indexer is in resume mode or not
    private boolean clean;                              //indicates if we have to index clean literals and entities

    /**
     * Constructor: this method will create the object and set the IndexWriter
     * @param indexPath string with the path to the directory where to store the index
     * @param similarity similarity that must be used during the indexing
     * @param analyzer analyzer that must be used during the indexing phase
     * @param resume boolean that indicates if we have to resume an indexing process
     * @param clean boolean that indicates if we have to index clean literals and entities
     */
    public DatasetIndexerLabels(String indexPath, Similarity similarity, Analyzer analyzer, boolean resume, boolean clean){
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
        this.clean = clean;
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

        for(File dataset: datasets){

            if (dataset.isDirectory() && !skipDatasets.contains(dataset.getName())) {

                //check if the dataset was already indexed

                boolean indexed = false;
                if (resume)
                    indexed = isIndexed(dataset);

                if (!indexed){

                    DatasetReader reader = new DatasetReader(dataset.getAbsolutePath());
                    DatasetMetaData metaData = reader.getMetaData();

                    //in this solution the files are mined only by RDFLib

                    DatasetContent contentRDFLibHR = reader.getContentRDFLibHR();
                    DatasetContent contentRDFLibHRClean = null;

                    if(clean)
                        contentRDFLibHRClean = reader.getContentRDFLibHRClean();

                    //check if the dataset is mined from LightRDF
                    Scanner entitiesFile = null;
                    Scanner classesFile = null;
                    Scanner literalsFile = null;
                    Scanner propertiesFile = null;

                    File entities = new File(dataset.getPath()+"/entities_lightrdf.txt");
                    File classes = new File(dataset.getPath()+"/classes_lightrdf.txt");
                    File literals = new File(dataset.getPath()+"/literals_lightrdf.txt");
                    File properties = new File(dataset.getPath()+"/properties_lightrdf.txt");

                    boolean bigDataset = false;

                    if(entities.exists()){
                        entitiesFile = new Scanner(entities);
                        classesFile = new Scanner(classes);
                        propertiesFile = new Scanner(properties);
                        literalsFile = new Scanner(literals);

                        bigDataset = isBigDataset(entities,properties, literals, classes);
                    }

                    try {
                        indexDataset(metaData, contentRDFLibHR, contentRDFLibHRClean, entitiesFile, classesFile, propertiesFile, literalsFile, bigDataset);

                    } catch (OutOfMemoryError e ){
                        System.out.println("Out of Memory in dataset:"+dataset.getName());

                    }

                    //force to release memory
                    contentRDFLibHRClean = null;
                    contentRDFLibHR = null;
                    System.gc();

                    //update the dataset_metadata.json file with the "indexed" field
                    updateMetadata(dataset);

                    //TODO: tune the parameter
                    if (datasetCount % 50 == 0)
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
     * @param contentRDFLibHR a DatasetContent object with all the dataset content extracted from RDFLib
     * @param contentRDFLibHRClean a DatasetContent object with all the dataset content literals and entities cleaned up
     * @param entitiesFile file with the entities extracted from LightRDF
     * @param classesFile file with the classes extracted from LightRDF
     * @param propertiesFile file with the properties extracted from LightRDF
     * @param literalsFile file with the literals extracted from LightRDF
     * @param bigDataset boolean that indicates if the dataset is big
     * @throws IOException if there are problems during the index writing of the dataset
     */
    private void indexDataset(DatasetMetaData metaData, DatasetContent contentRDFLibHR, DatasetContent contentRDFLibHRClean,
                              Scanner entitiesFile, Scanner classesFile, Scanner propertiesFile, Scanner literalsFile, boolean bigDataset) throws IOException {

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
            for(String dClass: contentRDFLibHR.classes)
                dataset.add(new DataField(DatasetFields.CLASSES, dClass));

            for(String property: contentRDFLibHR.properties)
                dataset.add(new DataField(DatasetFields.PROPERTIES, property));
        }

        if(contentRDFLibHRClean!=null){
            for(String entity: contentRDFLibHRClean.entities)
                dataset.add(new DataField(DatasetFields.ENTITIES, entity));

            for(String literal: contentRDFLibHRClean.literals)
                dataset.add(new DataField(DatasetFields.LITERALS, literal));

        }

        //release memory
        contentRDFLibHR = null;
        System.gc();

        //check if there are elements from LightRDF

        if(entitiesFile != null && !bigDataset) {
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

        if(entitiesFile != null && bigDataset) {

            int tripleLimit = 100000;

            int i = 0;
            while(entitiesFile.hasNext() && i < tripleLimit){
                dataset.add(new DataField(DatasetFields.ENTITIES, entitiesFile.nextLine().replace("\n", "")));
                i++;
            }

            i = 0;
            while(classesFile.hasNext() && i < tripleLimit){
                dataset.add(new DataField(DatasetFields.CLASSES, classesFile.nextLine().replace("\n", "")));
                i++;
            }

            i = 0;
            while(literalsFile.hasNext() && i < tripleLimit){
                dataset.add(new DataField(DatasetFields.LITERALS, literalsFile.nextLine().replace("\n", "")));
                i++;
            }

            i = 0;
            while(propertiesFile.hasNext() && i < tripleLimit){
                dataset.add(new DataField(DatasetFields.PROPERTIES, propertiesFile.nextLine().replace("\n", "")));
                i++;
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
     * This method will index a single dataset by removing duplicated classes
     *
     * @param metaData a DatasetMetaData object with all the dataset meta info
     * @param contentRDFLibHR a DatasetContent object with all the dataset content extracted from RDFLib
     * @param entitiesFile file with the entities extracted from LightRDF
     * @param classesFile file with the classes extracted from LightRDF
     * @param propertiesFile file with the properties extracted from LightRDF
     * @param literalsFile file with the literals extracted from LightRDF
     * @param bigDataset boolean that indicates if the dataset is big
     * @throws IOException if there are problems during the index writing of the dataset
     */
    private void indexDatasetNoDuplicatedClasses(DatasetMetaData metaData, DatasetContent contentRDFLibHR,
                              Scanner entitiesFile, Scanner classesFile, Scanner propertiesFile, Scanner literalsFile, boolean bigDataset) throws IOException {

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

        //add the content extracted by RDFLib

        if(contentRDFLibHR!=null){

            //create the class set for removing duplicates
            HashSet<String> classes = new HashSet<>(contentRDFLibHR.classes);

            for(String dClass: classes)
                dataset.add(new DataField(DatasetFields.CLASSES, dClass));

            for(String entity: contentRDFLibHR.entities)
                dataset.add(new DataField(DatasetFields.ENTITIES, entity));

            for(String literal: contentRDFLibHR.literals)
                dataset.add(new DataField(DatasetFields.LITERALS, literal));

            for(String property: contentRDFLibHR.properties)
                dataset.add(new DataField(DatasetFields.PROPERTIES, property));
        }

        //release memory
        contentRDFLibHR = null;
        System.gc();

        //check if there are elements from LightRDF

        if(entitiesFile != null && !bigDataset) {
            while(entitiesFile.hasNext()){
                dataset.add(new DataField(DatasetFields.ENTITIES, entitiesFile.nextLine().replace("\n", "")));
            }

            //remove the duplicated classes
            HashSet<String> classes = new HashSet<>();
            while(classesFile.hasNext()){
                classes.add(classesFile.nextLine().replace("\n", ""));
            }
            for(String dClass: classes)
                dataset.add(new DataField(DatasetFields.CLASSES, dClass));

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

        if(entitiesFile != null && bigDataset) {

            int tripleLimit = 100000;

            int i = 0;
            while(entitiesFile.hasNext() && i < tripleLimit){
                dataset.add(new DataField(DatasetFields.ENTITIES, entitiesFile.nextLine().replace("\n", "")));
                i++;
            }

            //remove the duplicated classes
            HashSet<String> classes = new HashSet<>();
            i = 0;
            while(classesFile.hasNext() && i < tripleLimit){
                classes.add(classesFile.nextLine().replace("\n", ""));
                i++;
            }
            for(String dClass: classes)
                dataset.add(new DataField(DatasetFields.CLASSES, dClass));

            i = 0;
            while(literalsFile.hasNext() && i < tripleLimit){
                dataset.add(new DataField(DatasetFields.LITERALS, literalsFile.nextLine().replace("\n", "")));
                i++;
            }

            i = 0;
            while(propertiesFile.hasNext() && i < tripleLimit){
                dataset.add(new DataField(DatasetFields.PROPERTIES, propertiesFile.nextLine().replace("\n", "")));
                i++;
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

        String indexPath = "/media/manuel/Tesi/Index/Index_HRv2_Stop_Clean";
        String datasetsDirectoryPath = "/media/manuel/Tesi/Datasets";
        //String datasetsDirectoryPath = "/home/manuel/Tesi/ACORDAR/Datasets";

        Analyzer a = CustomAnalyzer.getStopwordsAnalyzer();
        Similarity s = new BM25Similarity();

        boolean resume = false;
        boolean clean = true;
        DatasetIndexerLabels indexer = new DatasetIndexerLabels(indexPath, s, a, resume, clean);

        indexer.indexDatasets(datasetsDirectoryPath);

    }

}

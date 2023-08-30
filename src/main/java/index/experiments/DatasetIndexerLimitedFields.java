package index.experiments;

import analyze.CustomAnalyzer;

import index.DataField;
import index.MetadataField;
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
import java.io.IOException;
import java.util.HashSet;
import java.util.Scanner;

/**
 * This class will execute the indexing phase by considering only the literals and entities
 * extracted and cleaned from the rdflibhr_extractor.py
 *
 * @author Manuel Barusco
 * @version 1.0
 * @since 1.0
 */

public class DatasetIndexerLimitedFields {

    private File indexDirectory;                        //File object to the index directory
    private IndexWriter writer;                         //Lucene object for creating an index
    private IndexWriterConfig iwc;                      //IndexWriterConfig of the IndexWriter $writer wrapped inside
    private static final int RAMBUFFER_SIZE = 2048 ;    //RAMBuffer size limit for the index writer

    /**
     * Constructor: this method will create the object and set the IndexWriter
     * @param indexPath string with the path to the directory where to store the index
     * @param similarity similarity that must be used during the indexing
     * @param analyzer analyzer that must be used during the indexing phase
     * @param fields array of strings that contains the fields that must be indexed
     */
    public DatasetIndexerLimitedFields(String indexPath, Similarity similarity, Analyzer analyzer){
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
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setCommitOnClose(true);
        iwc.setUseCompoundFile(true);
    }

    /**
     * This method returns true if a given dataset is considered big (at least one of the input files is bigger
     * than 1 GB)
     *
     * @param literals File object that point to the list of literals in the dataset extracted with LightRDF
     * @return True if the dataset is considered big else False
     */
    public boolean isBigDataset(File literals){
        return (literals.length() / Math.pow(1024, 2) > 1000) ;
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

                DatasetReader reader = new DatasetReader(dataset.getAbsolutePath());
                DatasetMetaData metaData = reader.getMetaData();

                //in this solution the files are mined only by RDFLib
                DatasetContent contentRDFLibHRClean = reader.getContentRDFLibHRClean();

                Scanner literalsFile = null;

                File literals = new File(dataset.getPath()+"/literals_lightrdf.txt");

                boolean bigDataset = false;

                if(literals.exists()){
                    literalsFile = new Scanner(literals);

                    bigDataset = isBigDataset(literals);
                }

                try {
                    indexDataset(metaData, contentRDFLibHRClean, literalsFile, bigDataset);

                } catch (OutOfMemoryError e ){
                    System.out.println("Out of Memory in dataset:"+dataset.getName());

                }

                //force to release memory
                contentRDFLibHRClean = null;
                System.gc();

                //TODO: tune the parameter
                if (datasetCount % 50 == 0)
                    writer.commit();

            }

            if (datasetCount % 100 == 0)
                System.out.println("Indexed: "+datasetCount);

            datasetCount++;

        }

        writer.close();
    }


    /**
     * This method will index a single dataset
     *
     * @param metaData a DatasetMetaData object with all the dataset meta info
     * @param contentRDFLibHRClean a DatasetContent object with all the dataset content literals and entities cleaned up
     * @param literalsFile file with the literals extracted from LightRDF
     * @param bigDataset boolean that indicates if the dataset is big
     * @throws IOException if there are problems during the index writing of the dataset
     */
    private void indexDataset(DatasetMetaData metaData, DatasetContent contentRDFLibHRClean,
                              Scanner literalsFile, boolean bigDataset) throws IOException {

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

        if(contentRDFLibHRClean!=null){
            for(String entity: contentRDFLibHRClean.entities)
                dataset.add(new DataField(DatasetFields.ENTITIES, entity));

            for(String literal: contentRDFLibHRClean.literals)
                dataset.add(new DataField(DatasetFields.LITERALS, literal));

        }

        //release memory
        contentRDFLibHRClean = null;
        System.gc();

        //check if there are elements from LightRDF

        if(literalsFile != null && !bigDataset) {

            while(literalsFile.hasNext()){
                dataset.add(new DataField(DatasetFields.LITERALS, literalsFile.nextLine().replace("\n", "")));
            }

            literalsFile.close();

        }

        if(literalsFile != null && bigDataset) {

            int tripleLimit = 100000;

            int i = 0;
            while(literalsFile.hasNext() && i < tripleLimit){
                dataset.add(new DataField(DatasetFields.LITERALS, literalsFile.nextLine().replace("\n", "")));
                i++;
            }

            literalsFile.close();

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

        String indexPath = "/media/manuel/Tesi/Index/Index_HRv2_Stop_only_clean";
        String datasetsDirectoryPath = "/media/manuel/Tesi/Datasets";
        //String datasetsDirectoryPath = "/home/manuel/Tesi/ACORDAR/Datasets";

        Analyzer a = CustomAnalyzer.getStopwordsAnalyzer();
        Similarity s = new BM25Similarity();

        DatasetIndexerLimitedFields indexer = new DatasetIndexerLimitedFields(indexPath, s, a);

        indexer.indexDatasets(datasetsDirectoryPath);

    }

}

package parse;

import com.google.gson.Gson;
import utils.DatasetContent;
import utils.DatasetMetaData;

import java.io.*;

/**
 * This class will return the datasets content and meta-data associated
 *
 * @author Manuel Barusco
 * @version 1.0
 * @since 1.0
 */
public class DatasetReader {

    private String datasetDirectoryPath;                  //Path to the directory where the dataset is stored
    private DatasetMetaData metaData;                     //Meta-data associated to the dataset
    private DatasetContent content;                       //Content of the dataset
    private Reader metaDataReader;                        //FileReader to the meta-data file of the dataset
    private Reader contentReader;                         //FileReader to the content file of the dataset

    /**
     * Constructor
     *
     * @param datasetDirectoryPath path to the dataset directory
     */
    public DatasetReader(String datasetDirectoryPath){
        //check for the datasetDirectoryPath
        if(datasetDirectoryPath.isEmpty() || datasetDirectoryPath == null)
            throw new IllegalArgumentException("The dataset directory path cannot be null or empty");

        File datasetDirectory = new File(datasetDirectoryPath);
        if(!datasetDirectory.isDirectory() || !datasetDirectory.exists())
            throw new IllegalArgumentException("The dataset directory specified does not exist");

        this.datasetDirectoryPath = datasetDirectoryPath;

        //create the FileReader objects
        try{
            metaDataReader = new FileReader(datasetDirectoryPath+"/dataset_metadata.json");
            contentReader = new FileReader(datasetDirectoryPath+"/dataset_content.json");
        } catch (FileNotFoundException e) {
            throw new RuntimeException("The metadata or content file for the dataset at path: "+datasetDirectory+" is not present");
        }

    }

    /**
     * This method will return the dataset metadata
     * @return object of type DatasetMetaData
     * @throws IOException if there are problems when closing the metaDataReader object
     */
    public DatasetMetaData getMetaData() throws IOException {
        if (metaData == null) {
            Gson gson = new Gson();
            metaData = gson.fromJson(metaDataReader, DatasetMetaData.class);
            metaDataReader.close();
        }
        return metaData;
    }

    /**
     * This method will return the dataset content
     * @return object of type DatasetContent
     * @throws IOException if there are problems when closing the contentReadr object
     */
    public DatasetContent getContent() throws IOException {
        if (content == null) {
            Gson gson = new Gson();
            content = gson.fromJson(contentReader, DatasetContent.class);
            contentReader.close();
        }
        return content;
    }



}

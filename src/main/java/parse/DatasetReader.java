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

    }

    /**
     * This method will return the dataset metadata
     * @return object of type DatasetMetaData
     * @throws IOException if there are problems when closing the metaDataReader object
     */
    public DatasetMetaData getMetaData() throws IOException {
        FileReader metaDataReader = new FileReader(datasetDirectoryPath+"/dataset_metadata.json");
        Gson gson = new Gson();
        DatasetMetaData metaData = gson.fromJson(metaDataReader, DatasetMetaData.class);
        metaDataReader.close();

        return metaData;
    }

    /**
     * This method will return the dataset content extracted by JENA
     * @return object of type DatasetContent
     * @throws IOException if there are problems when closing the contentReadr object
     */
    public DatasetContent getContentJena() throws IOException {

        //check for the presence of the dataset_content_jena.json file
        File contentFileJena = new File(datasetDirectoryPath+"/dataset_content_jena.json");
        if (contentFileJena.exists()){
            FileReader contentReader = new FileReader(datasetDirectoryPath+"/dataset_content_jena.json");
            Gson gson = new Gson();
            DatasetContent contentJena = gson.fromJson(contentReader, DatasetContent.class);
            contentReader.close();

            return contentJena;
        }
        return null;
    }


    /**
     * This method will return the dataset content extracted by RDFLib
     * @return object of type DatasetContent with the data
     * @throws IOException if there are problems when closing the contentReadr object
     */
    public DatasetContent getContentRDFLib() throws IOException {
        //check for the presence of the dataset_content_rdflib.json file
        File contentFileRDFLib = new File(datasetDirectoryPath+"/dataset_content_rdflib.json");
        if (contentFileRDFLib.exists()){
            FileReader contentReader = new FileReader(datasetDirectoryPath+"/dataset_content_rdflib.json");
            Gson gson = new Gson();
            DatasetContent contentRDFLib = gson.fromJson(contentReader, DatasetContent.class);
            contentReader.close();

            return contentRDFLib;
        }
        return null;
    }

    /**
     * This method will return the dataset content extracted by RDFLibHR extractor
     * @return object of type DatasetContent with the data
     * @throws IOException if there are problems when closing the contentReadr object
     */
    public DatasetContent getContentRDFLibHR() throws IOException {
        String path = datasetDirectoryPath+"/dataset_content_rdflibhr.json";
        //check for the presence of the dataset_content_rdflibhr.json file
        File contentFileRDFLib = new File(path);
        if (contentFileRDFLib.exists()){
            FileReader contentReader = new FileReader(path);
            Gson gson = new Gson();
            DatasetContent contentRDFLib = gson.fromJson(contentReader, DatasetContent.class);
            contentReader.close();

            return contentRDFLib;
        }
        return null;
    }

    /**
     * This method will return the dataset content extracted by RDFLibHR and cleaned
     * @return object of type DatasetContent with the data
     * @throws IOException if there are problems when closing the contentReadr object
     */
    public DatasetContent getContentRDFLibHRClean() throws IOException {
        String path = datasetDirectoryPath+"/dataset_content_rdflibhr_clean.json";
        //check for the presence of the dataset_content_rdflibhr_clean.json file
        File contentFileRDFLibClean = new File(path);
        if (contentFileRDFLibClean.exists()){
            FileReader contentReader = new FileReader(path);
            Gson gson = new Gson();
            DatasetContent contentRDFLibClean = gson.fromJson(contentReader, DatasetContent.class);
            contentReader.close();

            return contentRDFLibClean;
        }
        return null;
    }


}

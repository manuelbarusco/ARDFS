package utils;

import parse.DatasetFields;

import javax.xml.crypto.Data;
import java.util.HashMap;

public class BoostWeights {
    public static final HashMap<String, Float> BM25BoostWeights;
    public static final HashMap<String, Float> BM25MetadataBoostWeights;
    public static final HashMap<String, Float> BM25DataBoostWeights;
    public static final HashMap<String, Float> TFIDFBoostWeights;
    public static final HashMap<String, Float> TFIDFMetadataBoostWeights;
    public static final HashMap<String, Float> TFIDFDataBoostWeights;
    public static final HashMap<String, Float> LMDBoostWeights;
    public static final HashMap<String, Float> LMDMetadataBoostWeights;
    public static final HashMap<String, Float> LMDDataBoostWeights;

    static {

        String[] queryFields = {DatasetFields.TITLE, DatasetFields.DESCRIPTION, DatasetFields.AUTHOR, DatasetFields.TAGS, DatasetFields.ENTITIES, DatasetFields.LITERALS, DatasetFields.CLASSES, DatasetFields.PROPERTIES};
        float[] BM25Weights = {1.0f, 0.9f, 0.9f, 0.6f, 0.2f, 0.3f, 0.1f, 0.1f};
        float[] TFIDFWeights = {1.0f, 0.7f, 0.9f, 0.9f, 0.8f, 0.5f, 0.1f, 0.4f};
        float[] LMDWeights = {1.0f, 0.9f, 0.1f, 1.0f, 0.2f, 0.3f, 0.2f, 0.1f};
        float[] FSDMWeights = {1.0f, 0.1f, 0.5f, 0.9f, 0.1f, 0.1f, 0.4f, 0.6f};

        float[] BM25MetadataWeights = {0.5f, 0.3f, 0.2f, 0.2f};
        float[] TFIDFMetadataWeights = {1.0f, 0.6f, 0.4f, 0.5f};
        float[] LMDMetadataWeights = {1.0f, 0.8f, 0.9f, 0.7f};
        float[] FSDMMetadataWeights = {1.0f, 0.1f, 0.2f, 0.6f};

        float[] BM25ContentWeights = {0.1f, 0.7f, 0.2f, 0.2f};
        float[] TFIDFContentWeights = {0.3f, 1.0f, 0.6f, 0.3f};
        float[] LMDContentWeights = {0.3f, 1.0f, 0.1f, 0.6f};
        float[] FSDMContentWeights = {1.0f, 0.6f, 0.1f, 0.1f};

        //setting the query boosting weights for BM25
        BM25BoostWeights = new HashMap<>();
        for(int i = 0; i < queryFields.length; i++){
            BM25BoostWeights.put(queryFields[i], BM25Weights[i]);
        }

        BM25MetadataBoostWeights = new HashMap<>();
        for(int i = 0, j = 0; i < 4; i++, j++){
            BM25MetadataBoostWeights.put(queryFields[i], BM25MetadataWeights[j]);
        }

        BM25DataBoostWeights = new HashMap<>();
        for(int i = 4, j = 0; i < queryFields.length; i++, j++){
            BM25DataBoostWeights.put(queryFields[i], BM25ContentWeights[j]);
        }

        //setting the query boosting weights for TFIDF
        TFIDFBoostWeights = new HashMap<>();
        for(int i = 0; i < queryFields.length; i++){
            TFIDFBoostWeights.put(queryFields[i], TFIDFWeights[i]);
        }

        TFIDFMetadataBoostWeights = new HashMap<>();
        for(int i = 0, j = 0; i < 4; i++, j++){
            TFIDFMetadataBoostWeights.put(queryFields[i], TFIDFMetadataWeights[j]);
        }

        TFIDFDataBoostWeights = new HashMap<>();
        for(int i = 4, j = 0; i < queryFields.length; i++, j++){
            TFIDFDataBoostWeights.put(queryFields[i], TFIDFContentWeights[j]);
        }

        //setting the query boosting weights for LMD
        LMDBoostWeights = new HashMap<>();
        for(int i = 0; i < queryFields.length; i++){
            LMDBoostWeights.put(queryFields[i], LMDWeights[i]);
        }

        LMDMetadataBoostWeights = new HashMap<>();
        for(int i = 0, j = 0; i < 4; i++, j++){
            LMDMetadataBoostWeights.put(queryFields[i], LMDMetadataWeights[j]);
        }

        LMDDataBoostWeights = new HashMap<>();
        for(int i = 4, j = 0; i < queryFields.length; i++, j++){
            LMDDataBoostWeights.put(queryFields[i], LMDContentWeights[j]);
        }

    }

}

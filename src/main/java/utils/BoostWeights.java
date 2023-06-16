package utils;

import parse.DatasetFields;

import java.util.HashMap;

public class BoostWeights {
    public static final HashMap<String, Float> BM25BoostWeights;
    public static final HashMap<String, Float> TFIDFBoostWeights;
    public static final HashMap<String, Float> LMDBoostWeights;

    static {
        //setting the query boosting weights
        BM25BoostWeights = new HashMap<>();

        BM25BoostWeights.put(DatasetFields.TITLE, 1f);
        BM25BoostWeights.put(DatasetFields.DESCRIPTION, 0.9f);
        BM25BoostWeights.put(DatasetFields.AUTHOR, 0.9f);
        BM25BoostWeights.put(DatasetFields.TAGS, 0.6f);
        BM25BoostWeights.put(DatasetFields.CLASSES, 0.2f);
        BM25BoostWeights.put(DatasetFields.ENTITIES, 0.3f);
        BM25BoostWeights.put(DatasetFields.LITERALS, 0.1f);
        BM25BoostWeights.put(DatasetFields.PROPERTIES, 0.1f);


        TFIDFBoostWeights = new HashMap<>();

        TFIDFBoostWeights.put(DatasetFields.TITLE, 1f);
        TFIDFBoostWeights.put(DatasetFields.DESCRIPTION, 0.7f);
        TFIDFBoostWeights.put(DatasetFields.AUTHOR, 0.9f);
        TFIDFBoostWeights.put(DatasetFields.TAGS, 0.9f);
        TFIDFBoostWeights.put(DatasetFields.CLASSES, 0.8f);
        TFIDFBoostWeights.put(DatasetFields.ENTITIES, 0.5f);
        TFIDFBoostWeights.put(DatasetFields.LITERALS, 0.1f);
        TFIDFBoostWeights.put(DatasetFields.PROPERTIES, 0.4f);


        LMDBoostWeights = new HashMap<>();

        LMDBoostWeights.put(DatasetFields.TITLE, 1f);
        LMDBoostWeights.put(DatasetFields.DESCRIPTION, 0.1f);
        LMDBoostWeights.put(DatasetFields.AUTHOR, 0.5f);
        LMDBoostWeights.put(DatasetFields.TAGS, 0.9f);
        LMDBoostWeights.put(DatasetFields.CLASSES, 0.1f);
        LMDBoostWeights.put(DatasetFields.ENTITIES, 0.1f);
        LMDBoostWeights.put(DatasetFields.LITERALS, 0.4f);
        LMDBoostWeights.put(DatasetFields.PROPERTIES, 0.6f);

    }

}

package index;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;

public class DatasetField extends Field {

    private static final FieldType type = new FieldType();

    static {
        type.setStored(true);
        type.setTokenized(true);
        type.setStoreTermVectors(true);
        type.setStoreTermVectorPositions(true);
        type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    }

    /**
     * Constructor
     * @param key name of the field
     * @param value value of the field
     */
    public DatasetField(final String key, final String value) {
        super(key, value, type);
    }
}

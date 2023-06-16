package index;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;

public class DataField extends Field {

    private static final FieldType type = new FieldType();

    static {
        type.setStored(false);
        type.setTokenized(true);
        type.setStoreTermVectors(false);
        type.setStoreTermVectorPositions(false);
        type.setIndexOptions(IndexOptions.DOCS);
    }

    /**
     * Constructor
     * @param key name of the field
     * @param value value of the field
     */
    public DataField(final String key, final String value) {
        super(key, value, type);
    }
}

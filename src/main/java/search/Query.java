package search;

/**
 * This class represent a simple query
 *
 * @author Manuel Barusco
 * @version 1.0
 * @since 1.0
 */
public class Query {
    public int id;          //id of the query
    public String text;     //text of the query

    public static class QUERY_FIELDS{
        public static final String ID = "query_id";
        public static final String TEXT = "description";
    }
}

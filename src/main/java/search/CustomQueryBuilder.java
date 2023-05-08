package search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanClause;

/**
 * This class is a custom Boolean Query Builder
 *
 * @author Manuel Barusco
 * @version 1.0
 * @since 1.0
 */
public class CustomQueryBuilder {


    /**
     * This method build a simple boolean
     * @param fields array of strings with all the fields name of a dataset document
     * @param analyzer that must be used in the query analysis
     * @param text of the query
     * @return the BooleanQuery
     */
    public static BooleanQuery buildBooleanQuery(String[] fields, Analyzer analyzer, String text) throws ParseException {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();

        if (text != null && !text.isEmpty() && !text.isBlank()) {

            for(String field : fields){
                builder.add(
                        new BooleanClause(
                                new QueryParser(field, analyzer).parse(QueryParser.escape(text)), BooleanClause.Occur.SHOULD
                        )
                );
            }
        }

        return builder.build();
    }

}

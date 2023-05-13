package search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;

import java.util.Map;

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

    /**
     * Parse multiple fields in the query using the implementation of {@code MultiFieldQueryParser} provided by the library
     * by considering the input query
     *
     * @param query The query to parse
     * @param analyzer that must be used for the parsing
     * @param queryWeights map with the query weights for boosting
     * @return a {@code Query} object
     * @throws ParseException if any error occurs while parsing the query given as parameter
     */
    public static Query buildBoostedQuery(String query, Analyzer analyzer, Map<String, Float> queryWeights) throws ParseException{
        String queryEscaped = QueryParserBase.escape(query);

        String[] fields = new String[queryWeights.size()];
        queryWeights.keySet().toArray(fields);

        MultiFieldQueryParser mqp = new MultiFieldQueryParser(fields, analyzer, queryWeights);
        return mqp.parse(queryEscaped);
    }

}

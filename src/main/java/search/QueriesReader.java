package search;

import org.apache.lucene.benchmark.quality.QualityQuery;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class QueriesReader {

    /**
     * Returns a list of {@code QualityQuery} representing the queries
     *
     * @param input the {@code BufferedReader}
     * @return an array of {@code QualityQuery}
     * @throws IOException if something goes wrong
     */
    public QualityQuery[] readQueries(BufferedReader input) throws IOException {

        List<QualityQuery> queryList = new ArrayList<>();

        String line;
        while((line = input.readLine())!=null){

            Scanner lineScanner = new Scanner(line);

            int id = lineScanner.nextInt();
            String text = StringUtils.substringAfter(line, String.valueOf(id));

            HashMap<String, String> fields = new HashMap<>();
            fields.put(DatasetSearcher.QUERY_FIELDS.TEXT, text);

            queryList.add(new QualityQuery(String.valueOf(id), fields));
        }

        QualityQuery[] queries = new QualityQuery[queryList.size()];
        queryList.toArray(queries);
        return queries;
    }

    //ONLY FOR DEBUG PURPOSE

    public static void main(String[] args) throws IOException {
        String queriesFilePath = "/home/manuel/Tesi/ACORDAR/Data/all_queries.txt";

        BufferedReader in = Files.newBufferedReader(Paths.get(queriesFilePath), StandardCharsets.UTF_8);
        QualityQuery[] queries = new QueriesReader().readQueries(in);  //list of topics

        for(int i = 0; i<queries.length; i++)
            System.out.println(queries[i].getValue("text"));

    }
}

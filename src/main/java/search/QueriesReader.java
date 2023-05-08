package search;

import org.apache.lucene.benchmark.quality.QualityQuery;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

public class QueriesReader {

    /**
     * Returns a list of {@code QualityQuery} representing the queries
     *
     * @param queriesPath path to the file that contains the queries
     * @return an array of {@code QualityQuery}
     * @throws IOException if something goes wrong
     */
    public QualityQuery[] readQueries(String queriesPath) throws IOException {
        //check the queries file path
        if(queriesPath==null || queriesPath.isEmpty())
            throw new IllegalArgumentException("The queries file path cannot be empty or null");

        File queriesFile = new File(queriesPath);
        if(!queriesFile.isFile() || !queriesFile.exists())
            throw new IllegalArgumentException("The queries file path not exist or does not point to a file");

        Scanner reader = new Scanner(queriesFile);
        List<QualityQuery> queryList = new ArrayList<>();

        while(reader.hasNextLine()){

            String[] queryFields = reader.nextLine().split("\t");

            HashMap<String, String> fields = new HashMap<>();

            //add the text
            fields.put(QueryFields.TEXT, queryFields[1]);

            queryList.add(new QualityQuery(String.valueOf(queryFields[0]), fields));
        }

        QualityQuery[] queries = new QualityQuery[queryList.size()];
        queryList.toArray(queries);
        return queries;
    }

    //ONLY FOR DEBUG PURPOSE

    public static void main(String[] args) throws IOException {
        String queriesFilePath = "/home/manuel/Tesi/ACORDAR/Data/all_queries.txt";

        QualityQuery[] queries = new QueriesReader().readQueries(queriesFilePath);  //list of topics

        for(int i = 0; i<queries.length; i++) {
            System.out.println(queries[i].getQueryID());
            System.out.println(queries[i].getValue(QueryFields.TEXT));
        }
    }
}

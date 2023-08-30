package search;

import analyze.CustomAnalyzer;
import javafx.util.Pair;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.benchmark.quality.QualityQuery;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import parse.DatasetFields;
import utils.BoostWeights;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * This class will implement the FSDM scoring function
 *
 * @author Manuel Barusco
 */
public class FSDMRanker {
    private static Directory directory = null;
    private static IndexReader indexReader = null;
    private static IndexSearcher indexSearcher = null;
    private Map<String, Double> wT;
    private Map<String, Double> wO;
    private Map<String, Double> wU;
    private Map<Pair<String, String>, Long> fieldTermFreq;
    private Map<String, List<String>> fieldContent;
    private Map<String, Long> fieldDocLength;
    private Analyzer analyzer;
    private int nHits;
    private HashMap<String, Float> boostWeights;
    private String[] fields;
    private final int FSDMUWindowSize = 8;

    /**
     * @param pathIndex path to the index diretory
     * @param analyzer to be used
     * @param nHits number of documents to be returned in the final rank
     */
    public FSDMRanker(String pathIndex, Analyzer analyzer, int nHits, HashMap<String, Float> boostWeights) {
        try {
            Path path = Paths.get(pathIndex);
            directory = MMapDirectory.open(path);
            indexReader = DirectoryReader.open(directory);
            indexSearcher = new IndexSearcher(indexReader);
            this.analyzer = analyzer;
            this.boostWeights = boostWeights;
            this.fields = this.boostWeights.keySet().toArray(String[]::new);
            this.nHits = nHits;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * This method break a given string into tokens by using the provided Analyzer
     * @param text given string text
     * @return list of tokens as a list of strings
     * @throws IOException
     */
    private List<String> getTokens(String text) throws IOException {
        List<String> res = new ArrayList<>();
        res.clear();
        TokenStream tokenStream = analyzer.tokenStream("", new StringReader(text));
        tokenStream.reset();
        CharTermAttribute charTerm = tokenStream.addAttribute(CharTermAttribute.class);
        while (tokenStream.incrementToken()) {
            res.add(charTerm.toString());
        }
        tokenStream.close();
        return res;
    }

    /**
     * This method set some collection statistics values useful for the next calculations
     * @param tokens list of query tokens
     */
    private void getCollectionStatistics(List<String> tokens) {
        try {
            wT = new HashMap<>();
            wO = new HashMap<>();
            wU = new HashMap<>();

            //this map contains key: pair (field, token) value: total frequency of the token in the field in the collection
            fieldTermFreq = new HashMap<>();

            double base = 0.0;
            for (int i = 0; i < fields.length; i ++) {
                String field = fields[i];
                float w = boostWeights.get(field);
                base += w;
                for (String token : tokens) {
                    fieldTermFreq.put(new Pair<>(field, token), indexReader.totalTermFreq(new Term(field, new BytesRef(token))));
                }
            }

            //retrieve the FSDM field weights for every component
            for (int i = 0; i < fields.length; i ++) {
                String field = fields[i];
                Double w = (double) boostWeights.get(field) / base;
                wT.put(field, w);
                wO.put(field, w);
                wU.put(field, w);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * This method set some document statistics values useful for the next calculations
     * from the document of id docId
     * @param docId document id
     */
    private void getDocumentStatistics(int docId) {
        try {
            fieldContent = new HashMap<>();
            fieldDocLength = new HashMap<>();

            Document document = indexReader.document(docId);
            for (String field : fields)  {
                if (document.get(field) == null)
                    fieldContent.put(field, new ArrayList<>());
                else {
                    String fieldText = Arrays.toString(document.getValues(field));
                    fieldContent.put(field, getTokens(fieldText));
                }
                Terms terms = indexReader.getTermVector(docId, field);
                if (terms != null)
                    fieldDocLength.put(field, terms.getSumTotalTermFreq());
                else
                    fieldDocLength.put(field, 0L);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * This method return the number of occurrences of a given query token
     * in a given field for a given document
     * @param docId id of the document
     * @param field field name
     * @param qi query token
     * @return number of occurrences of query token $qi in $field of document $docId
     */
    private Double getTF_T(Integer docId, String field, String qi) {
        double res = 0.0;
        try {
            Terms terms = indexReader.getTermVector(docId, field);
            BytesRef bytesRef = new BytesRef(qi);
            if (terms != null) {
                TermsEnum termsEnum = terms.iterator();
                if (termsEnum.seekExact(bytesRef))
                    res = (double) termsEnum.totalTermFreq();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        //System.out.println(qi + " TF_T: " + res);
        return res;
    }

    /**
     * This method return the number of occurrences of a given couple of query tokens
     * q_i and q_i+1
     * @param field field name
     * @param qi1 first query token
     * @param qi2 second query token
     */
    private Double getTF_O(String field, String qi1, String qi2) {
        double res = 0.0;
        List<String> content = fieldContent.get(field);
        for (int i = 0; i + 1 < content.size(); i++) {
            if (qi1.equals(content.get(i)) && qi2.equals(content.get(i + 1)))
                res += 1.0;
        }
        //System.out.println(qi1 + " " + qi2 + " TF_O: " + res);
        return res;
    }

    /**
     * This method return the number of occurrences of a given couple of query tokens
     * in a window of 8 terms
     * @param field field name
     * @param qi1 first query token
     * @param qi2 second query token
     */
    private Double getTF_U(String field, String qi1, String qi2) {
        double res = 0.0;
        List<String> content = fieldContent.get(field);
        for (Integer i = 0; i + FSDMUWindowSize <= content.size(); i++) {
            Set<String> window = new HashSet<>();
            for (Integer j = 0; j < FSDMUWindowSize;  j++) {
                window.add(content.get(i + j));
            }
            if (window.contains(qi1) && window.contains(qi2))
                res += 1.0;
        }
        //System.out.println(qi1 + " " + qi2 + " TF_U: " + res);
        return res;
    }

    /**
     * This method calculate the first component of FSDM
     * @param docId id of the document
     * @param query_tokens list of query tokens
     * @return FSDM unigram scores
     */
    private double getFSDM_T(Integer docId, List<String> query_tokens) {
        double res = 0.0;
        try {
            for (String qi : query_tokens) {
                double tmp = 0.0;
                double eps = 1e-100;
                for (String field : fields) {
                    if (wT.get(field) == 0.0)
                        continue;

                    //calculate the Dirichlet prior (average length of the field in the collection)
                    double miu = (double) indexReader.getSumTotalTermFreq(field) / (double) indexReader.getDocCount(field);

                    //total number of terms in the field in all the collection
                    double Cj = (double) indexReader.getSumTotalTermFreq(field);
                    double cf = 0.0;

                    //retrieve the frequency of the term in the field in all the collection
                    if (fieldTermFreq.containsKey(new Pair<>(field, qi)))
                        cf = (double) fieldTermFreq.get(new Pair<>(field, qi));

                    //size of the field in the document
                    double Dj = (double) fieldDocLength.get(field);

                    //get the term frequency in the document field and update the tmp value
                    tmp += wT.get(field) * (getTF_T(docId, field, qi) + miu * cf / Cj) / (Dj + miu);
                }
                res += Math.log(tmp + eps);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }

    /**
     * This method calculate the second component of FSDM
     * @param query_tokens list of query tokens
     * @return FSDM ordered bigram score
     */
    private Double getFSDM_O(List<String> query_tokens) {
        String[] fields = boostWeights.keySet().toArray(String[]::new);
        double res = 0.0;
        try {
            for (int i = 0; i + 1 < query_tokens.size(); i++) {
                double tmp = 0.0;
                double eps = 1e-100;

                //we are considering q_i and q_i+1
                String qi1 = query_tokens.get(i);
                String qi2 = query_tokens.get(i + 1);

                for (String field : fields) {
                    if (wO.get(field) == 0.0)
                        continue;
                    double miu = (double) indexReader.getSumTotalTermFreq(field) / (double) indexReader.getDocCount(field);
                    double Cj = (double) indexReader.getSumTotalTermFreq(field);
                    double cf = 0.0;
                    if (fieldTermFreq.containsKey(new Pair<>(field, qi1)))
                        cf = (double) fieldTermFreq.get(new Pair<>(field, qi1));
                    if (fieldTermFreq.containsKey(new Pair<>(field, qi2)))
                        cf = Math.min(cf, (double) fieldTermFreq.get(new Pair<>(field, qi2)));
                    double Dj = (double) fieldDocLength.get(field);
                    tmp += wO.get(field) * (getTF_O(field, qi1, qi2) + miu * cf / Cj) / (Dj + miu);
                }
                //System.out.println("O: " + tmp);
                res += Math.log(tmp + eps);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        //System.out.println("FSDM_O: " + res);
        return res;
    }

    /**
     * This method calculate the third component of FSDM
     * @param query_tokens list of query tokens
     * @return FSDM unordered bigram score
     */
    private Double getFSDM_U(List<String> query_tokens) {
        String[] fields = boostWeights.keySet().toArray(String[]::new);
        double res = 0.0;
        try {
            for (int i = 0; i + 1 < query_tokens.size(); i++) {
                double tmp = 0.0;
                double eps = 1e-100;
                String qi1 = query_tokens.get(i);
                String qi2 = query_tokens.get(i + 1);

                for (String field : fields) {
                    if (wU.get(field) == 0.0) continue;
                    double miu = (double) indexReader.getSumTotalTermFreq(field) / (double) indexReader.getDocCount(field);
                    double Cj = (double) indexReader.getSumTotalTermFreq(field);
                    double cf = 0.0;
                    if (fieldTermFreq.containsKey(new Pair<>(field, qi1)))
                        cf = (double) fieldTermFreq.get(new Pair<>(field, qi1));
                    if (fieldTermFreq.containsKey(new Pair<>(field, qi2)))
                        cf = Math.min(cf, (double) fieldTermFreq.get(new Pair<>(field, qi2)));
                    double Dj = (double) fieldDocLength.get(field);
                    tmp += wU.get(field) * (getTF_U(field, qi1, qi2) + miu * cf / Cj) / (Dj + miu);
                }
                //System.out.println("U: " + tmp);
                res += Math.log(tmp + eps);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        //System.out.println("FSDM_U: " + res);
        return res;
    }

    /**
     * This method calculate the FSDM score for every document returned in a given query rank
     * @param docId id of the document
     * @param tokens query tokens found by the Analyzer
     * @return FSDM score for the document of id docId and for the query given in input as a set of tokens
     */
    public Double FSDM(Integer docId, List<String> tokens) {
        Double lambdaT = 0.8;
        Double lambdaO = 0.1;
        Double lambdaU = 0.1;
        getCollectionStatistics(tokens);
        getDocumentStatistics(docId);
        return lambdaT * getFSDM_T(docId, tokens) +
                lambdaO * getFSDM_O(tokens) +
                lambdaU * getFSDM_U(tokens);
    }

    /**
     * This method returns the rank for the input query based on the FSDM ranking function
     * @param query string with the given query
     * @return rank for the query in the form of list of dataset-id, score and ordered by score
     */
    public List<Pair<Integer, Double>> getFSDMRankingList(String query) {
        //list for the final rank
        List<Pair<Integer, Double>> FSDMScoreList = new ArrayList<>();

        //get the fields where to search
        String[] fields = boostWeights.keySet().toArray(String[]::new);

        try {
            //create the query object
            QueryParser queryParser = new MultiFieldQueryParser(fields, analyzer);
            query=QueryParser.escape(query);
            Query parsedQuery = queryParser.parse(query);

            //search for the docs
            TopDocs docsSearch = indexSearcher.search(parsedQuery, nHits);
            ScoreDoc[] scoreDocs = docsSearch.scoreDocs;

            //get the query tokens
            List<String> queryTokens = getTokens(query);

            for (ScoreDoc si : scoreDocs) {
                int docID = si.doc;
                Set<String> fieldsToLoad = new HashSet<>();
                fieldsToLoad.add(DatasetFields.ID);

                Document document = indexReader.document(docID, fieldsToLoad);

                Integer datasetID = Integer.parseInt(document.get(DatasetFields.ID));
                Double score = 0.0;

                score = FSDM(docID, queryTokens);
                FSDMScoreList.add(new Pair<>(datasetID, score));
            }
            FSDMScoreList.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return FSDMScoreList;
    }

    public static void main(String[] args) throws IOException {
        String queryPath = "/home/manuel/Tesi/ACORDAR/Data/all_queries.txt";
        String indexPath = "/media/manuel/Tesi/Index/Stream_Jena_LightRDF_Deduplication";
        String resultDirectoryPath = "/home/manuel/Tesi/ACORDAR/Run/ARDFS";
        Analyzer analyzer = CustomAnalyzer.getStopwordsAnalyzer();
        String runID = "FSDM[d]";
        int nHits = 10;
        HashMap<String, Float> boostWeights = BoostWeights.FSDMDataBoostWeights;

        FSDMRanker ranker = new FSDMRanker(indexPath, analyzer, nHits, boostWeights);

        QualityQuery[] queries = new QueriesReader().readQueries(queryPath);

        PrintWriter writer = new PrintWriter(resultDirectoryPath+"/"+runID+".txt");

        for(QualityQuery query: queries){
            String textQuery = query.getValue(QueryFields.TEXT);
            String queryID = query.getQueryID();

            System.out.println("Searching query: "+queryID);

            List<Pair<Integer, Double>> rank = ranker.getFSDMRankingList(textQuery);

            //print results
            for(int i=0; i<rank.size(); i++){
                    writer.printf(Locale.ENGLISH, "%s\tQ0\t%s\t%d\t%.6f\t%s%n", queryID, rank.get(i).getKey(), i, rank.get(i).getValue(), runID);
                writer.flush();
            }
        }

    }

}

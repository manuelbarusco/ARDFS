package analyze;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.CharArraySet;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class CustomAnalyzer {

    public static Analyzer getStopwordsAnalyzer(){
        File stopListFile = new File("/home/manuel/Tesi/Codebase/EDS/src/main/java/resource/stopwords/nltk-stopwords.txt");
        List<String> stopWordsList = new ArrayList<>();

        try {
            Scanner stopWordsStream = new Scanner(stopListFile);

            while (stopWordsStream.hasNextLine()) {
                String stopWord = stopWordsStream.nextLine();
                stopWordsList.add(stopWord);
            }
        } catch (FileNotFoundException e){
            throw new RuntimeException("StopList file not found");
        }

        CharArraySet cas = new CharArraySet(0,true);
        cas.addAll(stopWordsList);
        return new StandardAnalyzer(cas);
    }

}

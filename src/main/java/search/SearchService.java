package search;

import com.google.common.collect.Sets;
import mapreduce.WordMapper;
import models.Article;
import models.Word;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;
import secondarysort.WikiPartitioner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * @author Shu Lin Chan, Jonathan Maeda, James Wang, Yaoming Zhan
 * Final Project
 */
@Service
public class SearchService {
    @Autowired
    private JavaSparkContext sc;

    public SearchService() { }

    private Article getArticle(int id) throws IOException, InterruptedException {
        Process p = Runtime.getRuntime().exec("/class/cs132/get_wiki_by_id " + id);
        p.waitFor();
        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        return new Article(id, reader.readLine(), reader.readLine(), reader.readLine());
    }

    public List<String> search(String terms) {
        String[] strings = terms.split("\\s+");
        List<String> words = new ArrayList<>();
        for (String string : strings) {
            String s = WordMapper.stem(string.toLowerCase().trim());

            if (s.equals("&") || s.equals("|") || s.equals("-")) continue;
            words.add(s);
        }

        List<String> queryResult = new ArrayList<>();
        words.forEach(word -> {
            List<String> result = sc.textFile(getFile(word)).filter(line -> words.contains(line.split("\\s+")[0])).cache().collect();
            if (!result.isEmpty()) queryResult.add(result.get(0));
        });

        return queryResult;
//        List<Set<String>> map = new ArrayList<>();
//
//        queryResult.forEach(word -> {
//            String[] positions = word.getPositions().split(";");
//            Set<String> appearances = new HashSet<>();
//            for (String pos : positions) {
//                String docId = pos.substring(0, pos.indexOf("."));
//                appearances.add(docId);
//            }
//            map.add(appearances);
//        });
//
//        Set<String> result = map.get(map.size() - 1);
//        int positionCount = map.size() - 2;
//        for (int i = 1; i < strings.length; i++) {
//            String current = strings[i++];
//            if ("&".equals(current)) {
//                result = Sets.intersection(result, map.get(positionCount--));
//            } else if ("|".equals(current)) {
//                result = Sets.union(result, map.get(positionCount--));
//            } else if ("-".equals(current)) {
//                result = Sets.difference(result, map.get(positionCount--));
//            } else {
//                result = Sets.union(result, map.get(positionCount--));
//                i--;
//            }
//        }
    }

    private String getFile(String word) {
        int hash = WikiPartitioner.getHash(word, 676);
        return "./output/part-r-00" + padZeroes(hash);
    }

    private String padZeroes(int num) {
        String n = String.valueOf(num);
        StringBuilder sb = new StringBuilder();
        for (int i = n.length(); i < 3; i++) {
            sb.append("0");
        }
        return sb.append(n).toString();
    }

    private void stop() {
        sc.stop();
    }

    public static void main(String[] args) {
        SearchService searcher = new SearchService();
        searcher.search(args[0]);
        searcher.stop();
    }
}
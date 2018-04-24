package search;

import com.google.common.collect.Sets;
import mapreduce.WordMapper;
import models.Article;
import models.Word;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
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

    public List<Article> search(String terms) throws IOException, InterruptedException {
        String[] strings = terms.split("\\s+");
        List<String> words = new ArrayList<>();
        for (String string : strings) {
            String s = WordMapper.stem(string.toLowerCase().trim());

            if (s.equals("&") || s.equals("|") || s.equals("~")) continue;
            words.add(s);
        }

        List<Set<String>> map = new ArrayList<>();

        words.forEach(word -> sc.textFile(getFile(word)).cache().filter(line -> words.contains(line.split("\\s+")[0]))
            .map(line -> {
                String[] split = line.split("\\s+");
                return new Word(split[0], split[1]);
            })
            .collect()
            .forEach(w -> {
                String[] positions = w.getPositions().split(";");
                Set<String> appearances = new HashSet<>();
                for (String pos : positions) {
                    String docId = pos.substring(0, pos.indexOf("."));
                    appearances.add(docId);
                }
                map.add(appearances);
            }));
        System.out.println("Completed Search");
        
        Set<String> result = map.get(map.size() - 1);
        int positionCount = map.size() - 2;
        for (int i = 1; i < strings.length; i++) {
            String current = strings[i++];
            if ("&".equals(current)) {
                result = Sets.intersection(result, map.get(positionCount--));
            } else if ("|".equals(current)) {
                result = Sets.union(result, map.get(positionCount--));
            } else if ("~".equals(current)) {
                result = Sets.difference(result, map.get(positionCount--));
            } else {
                result = Sets.union(result, map.get(positionCount--));
                i--;
            }
        }
        System.out.println("Completed Set Combining");
        List<Article> articleList = new ArrayList<>();
        for (String s : result) {
            System.out.println(s);
            articleList.add(getArticle(Integer.valueOf(s)));
        }
        System.out.println("Completed Getting Articles");
        System.out.println(articleList.size());
        return articleList;
    }

    private Article getArticle(int id) throws IOException, InterruptedException {
    	System.out.println("Getting Article" + id);
        Process p = Runtime.getRuntime().exec("/class/cs132/get_wiki_by_id " + id);
        p.waitFor();
        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        return new Article(id, reader.readLine(), reader.readLine(), reader.readLine());
    }

//    private Article getArticle(int id) {
//        return new Article(id, "http://www.google.com", "Title", "Content");
//    }

    private String getFile(String word) {
        StringBuilder sb = new StringBuilder(System.getenv("HDFS_URL"));
        int hash = WikiPartitioner.getHash(word, 676);
        String n = String.valueOf(hash);
        for (int i = n.length(); i < 3; i++) {
            sb.append('0');
        }
        return sb.append(n).toString();
    }

    private void stop() {
        sc.stop();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        SearchService searcher = new SearchService();
        searcher.search(args[0]);
        searcher.stop();
    }
}
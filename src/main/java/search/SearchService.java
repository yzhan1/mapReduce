package search;

import com.google.common.collect.Sets;
import mapreduce.WordMapper;
import models.Article;
import models.Word;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * @author Shu Lin Chan, Jonathan Maeda, James Wang, Yaoming Zhan
 * Final Project
 */
//@Service
public class SearchService {
    private SparkSession spark = SparkSession.builder().appName("cs132g4-WordSearcher").master("local[4]").getOrCreate();

    public SearchService() {
        loadIndex();
    }

    private void loadIndex() {
        Dataset<Row> wordDF;
        // TODO: Change to HDFS for cluster
//    try {
//      System.out.println("Reading from saved file");
//      wordDF = spark.read().parquet("./words.parquet").cache();
//    } catch (Exception e) {
//
//      wordDF.write().parquet("./words.parquet");
//    }
        System.out.println("Loading from file");


        JavaRDD<Word> wordRDD = spark.read()
//      .textFile("./output/")
            .textFile("/user/cs132g4/output8/part-r-00023")
            .javaRDD()
            .map(line -> {
                String[] parts = line.split("\\s+");
                return new Word(parts[0], parts[1]);
            }).cache();

        wordDF = spark.createDataFrame(wordRDD, Word.class).cache();
        wordDF.createOrReplaceTempView("words");
    }

    private Article getArticle(int id) throws IOException, InterruptedException {
        Process p = Runtime.getRuntime().exec("/class/cs132/get_wiki_by_id " + id);
        p.waitFor();
        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        return new Article(id, reader.readLine(), reader.readLine(), reader.readLine());
    }

    public void search(String terms) {
        String[] strings = terms.split("\\s+");
        StringBuilder query = new StringBuilder("SELECT * FROM words WHERE ");
        for (int i = 0; i < strings.length; i++) {
            String s = WordMapper.stem(strings[i].toLowerCase().trim());

            if (s.equals("&") || s.equals("|") || s.equals("-")) continue;
            if (i != 0) query.append("OR ");
            query.append("word = '").append(s).append("' ");
        }

        List<Row> queryResult = spark.sql(query.toString()).collectAsList();

        List<Set<String>> map = new ArrayList<>();

        queryResult.forEach(row -> {
            String[] positions = row.getAs("positions").toString().split(";");
            Set<String> appearances = new HashSet<>();
            for (String pos : positions) {
                String docId = pos.substring(0, pos.indexOf("."));
                appearances.add(docId);
            }
            map.add(appearances);
        });

        Set<String> result = map.get(map.size() - 1);
        int positionCount = map.size() - 2;
        for (int i = 1; i < strings.length; i++) {
            String current = strings[i++];
            if ("&".equals(current)) {
                result = Sets.intersection(result, map.get(positionCount--));
            } else if ("|".equals(current)) {
                result = Sets.union(result, map.get(positionCount--));
            } else if ("-".equals(current)) {
                result = Sets.difference(result, map.get(positionCount--));
            } else {
                result = Sets.union(result, map.get(positionCount--));
                i--;
            }
        }

//    result.forEach(r -> {
//      try {
//        System.out.println(getArticle(Integer.valueOf(r)));
//      } catch (IOException | InterruptedException e) {
//        e.printStackTrace();
//      }
//    });
        result.forEach(System.out::println);
    }

    private void stop() {
        spark.stop();
    }

    public static void main(String[] args) {
        SearchService searcher = new SearchService();
        searcher.search(args[0]);
        searcher.stop();
    }
}
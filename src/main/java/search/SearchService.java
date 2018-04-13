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

import java.io.File;
import java.util.*;

/**
 * @author Shu Lin Chan, Jonathan Maeda, James Wang, Yaoming Zhan
 * Final Project
 */
@Service
public class SearchService {
  private SparkSession spark = SparkSession.builder().appName("cs132g4-WordSearcher").master("local[4]").getOrCreate();
  private JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
  private JavaPairRDD<Integer, Article> articleRDD;

  public SearchService() {
    loadIndex();
    loadArticles();
  }

  private void loadIndex() {
    Dataset<Row> wordDF;
    // TODO: Change to HDFS for cluster
    try {
      System.out.println("Reading from saved file");
      wordDF = spark.read().parquet("hdfs://cs132g4/words.parquet").cache();
    } catch (Exception e) {
      System.out.println("Loading from file");

      JavaRDD<Word> wordRDD = spark.read()
        .textFile("hdfs://cs132g4/output7")
        .javaRDD()
        .map(line -> {
          String[] parts = line.split("\\s+");
          return new Word(parts[0], parts[1]);
        }).cache();

      wordDF = spark.createDataFrame(wordRDD, Word.class).cache();
      wordDF.write().parquet("hdfs://cs132g4/words.parquet");
    }
    wordDF.createOrReplaceTempView("words");
  }

  private void loadArticles() {
//    // TODO: Change to HDFS for cluster
//    if ((new File("./articles")).exists()) {
//      System.out.println("Loading article from saved file");
//      articleRDD = JavaPairRDD.fromJavaRDD(sc.objectFile("hdfs://cs132g4/articles"));
//    } else {
//
//    }
    System.out.println("Loading article from csv file");
    articleRDD = spark.read()
      .textFile("hdfs://data/wiki_csv")
      .javaRDD()
      .map(line -> line.split(","))
      .mapToPair(s -> new Tuple2<>(Integer.valueOf(s[0]), new Article(Integer.valueOf(s[0]), s[1], s[2], s[3]))).cache();

    articleRDD.saveAsObjectFile("hdfs://cs132g4/articles");
  }

  private Article getArticle(int id) {
    return articleRDD.cache().lookup(id).get(0);
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

    result.forEach(r -> System.out.println(getArticle(Integer.valueOf(r))));
  }

  public void stop() {
    spark.stop();
  }

  public static void main(String[] args) {
    SearchService searcher = new SearchService();
    searcher.search("abrar & ababa | aaa");
    searcher.stop();
  }
}
package search;

import com.google.common.collect.Sets;
import mapreduce.WordMapper;
import models.Article;
import models.Word;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.io.File;
import java.util.*;

public class WordSearcher {
  private SparkSession spark = SparkSession.builder().appName("cs132g4-WordSearcher").master("local[4]").getOrCreate();
  private JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

  public WordSearcher() {
    loadIndex();
  }

  private void loadIndex() {
    Dataset<Row> wordDF;
    // TODO: Change to HDFS for cluster
    try {
      System.out.println("Reading from saved file");
      wordDF = spark.read().parquet("./words.parquet").cache();
    } catch (Exception e) {
      System.out.println("Loading from file");

      JavaRDD<Word> wordRDD = spark.read()
        .textFile("./output/part-r-00000")
        .javaRDD()
        .map(line -> {
          String[] parts = line.split("\\s+");
          return new Word(parts[0], parts[1]);
        }).cache();

      wordDF = spark.createDataFrame(wordRDD, Word.class).cache();
      wordDF.write().parquet("./words.parquet");
    }
    wordDF.createOrReplaceTempView("words");
  }

  private Article getArticle(int id) {
    JavaPairRDD<Integer, Article> articleRDD;

    // TODO: Change to HDFS for cluster
    if ((new File("./articles")).exists()) {
      System.out.println("Loading article from saved file");
      articleRDD = JavaPairRDD.fromJavaRDD(sc.objectFile("./articles"));
    } else {
      System.out.println("Loading article from csv file");
      articleRDD = spark.read()
        .textFile("./data/wiki_00.csv")
        .javaRDD()
        .map(line -> line.split(","))
        .mapToPair(s -> new Tuple2<>(Integer.valueOf(s[0]), new Article(Integer.valueOf(s[0]), s[1], s[2], s[3]))).cache();

      articleRDD.saveAsObjectFile("./articles");
    }
    articleRDD.cache();

    return articleRDD.lookup(id).get(0);
  }

  public void search(String terms) {
    String[] strings = terms.split("\\s+");
    StringBuilder query = new StringBuilder();
    query.append("SELECT * FROM words WHERE ");
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

    Set<String> result = map.get(0);
    int positionCount = 0;
    for (int i = 0; i < strings.length; i++) {
      String current = strings[i++];
      if ("&".equals(current)) {
        result = Sets.intersection(result, map.get(positionCount++));
      } else if ("|".equals(current)) {
        result = Sets.union(result, map.get(positionCount++));
      } else if ("-".equals(current)) {
        result = Sets.difference(result, map.get(positionCount++));
      } else {
        result = Sets.union(result, map.get(positionCount++));
        i--;
      }
    }

    result.forEach(r -> System.out.println(getArticle(Integer.valueOf(r))));
  }

  public void stop() {
    spark.stop();
  }

  public static void main(String[] args) {
    WordSearcher searcher = new WordSearcher();
    searcher.search("abrar & ababa | aaa");
    searcher.stop();
  }
}
package search;

import com.google.common.collect.Sets;
import models.Word;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.util.*;

public class WordSearcher {
  private SparkSession spark = SparkSession.builder().appName("WordSearcher").master("local[4]").getOrCreate();

  public WordSearcher() {
    Dataset<Row> wordDF;
    try {
      System.out.println("Reading from saved file");
      wordDF = spark.read().parquet("./words.parquet");
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

  public void search(String terms) {
    String[] strings = terms.split("\\s+");
    StringBuilder query = new StringBuilder();
    query.append("SELECT * FROM words WHERE ");
    boolean first = true;
    for (String s : strings) {
      if (s.equals("&") || s.equals("|") || s.equals("-")) {
        continue;
      }
      if (!first) query.append("OR ");
      query.append("word = '").append(s).append("' ");
      first = false;
    }
    Dataset<Row> resultsDF = spark.sql(query.toString());

    List<Row> res = resultsDF.collectAsList();

    List<Set<String>> map = new ArrayList<>();

    res.forEach(row -> {
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
      switch (current) {
        case "&":
          result = Sets.intersection(result, map.get(positionCount++));
          break;
        case "|":
          result = Sets.union(result, map.get(positionCount++));
          break;
        case "-":
          result = Sets.difference(result, map.get(positionCount++));
          break;
        default:
          result = Sets.union(result, map.get(positionCount++));
          i--;
          break;
      }
    }

    for (String val : result) {
      System.out.println(val);
    }
  }

  public void stop() {
    spark.stop();
  }

  public static void main(String[] args) {
    WordSearcher searcher = new WordSearcher();
    searcher.search("abrar & ababa");
    searcher.stop();
  }
}
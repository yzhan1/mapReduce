package search;

import models.Word;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

public class WordSearcher {
  private SparkSession spark = SparkSession.builder().appName("WordSearcher").master("local[4]").getOrCreate();

  public WordSearcher() {
    JavaRDD<Word> wordRDD = spark.read()
      .textFile("./output/part-r-00000")
      .javaRDD()
      .map(line -> {
        String[] parts = line.split("\\s+");
        return new Word(parts[0], parts[1]);
      }).cache();

    Dataset<Row> wordDF = spark.createDataFrame(wordRDD, Word.class).cache();
    wordDF.createOrReplaceTempView("words");
  }

  public void search(String term) {
    Dataset<Row> resultsDF = spark.sql("SELECT * FROM words WHERE word LIKE CONCAT('%', '" + term + "', '%')");
    Encoder<String> stringEncoder = Encoders.STRING();

    resultsDF.show();
//    Dataset<String> searchResultsDF = resultsDF.map(
//      (MapFunction<Row, String>) row -> "Word: " + row.getAs("word") + " Pos: " + row.getAs("positions"),
//      stringEncoder
//    );
//    searchResultsDF.show();
  }

  public void stop() {
    spark.stop();
  }

  public static void main(String[] args) {
    WordSearcher searcher = new WordSearcher();
    searcher.search("aaa");
    searcher.stop();
  }
}
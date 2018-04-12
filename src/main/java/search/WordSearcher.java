package search;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.io.Serializable;

public class WordSearcher {
  public static void main(String[] args) {
    String term = "aaa";
    String logFile = "./output/part-r-00000";
    SparkSession spark = SparkSession.builder().appName("WordSearcher").master("local[4]").getOrCreate();
    Dataset<String> logData = spark.read().textFile(logFile).cache();

    System.out.println(logData.filter((FilterFunction<String>) s -> s.contains(term)).first());

    JavaRDD<Word> wordRDD = spark.read()
      .textFile("./output/part-r-00000")
      .javaRDD()
      .map((line) -> {
        String[] parts = line.split("\\s+");
        Word word = new Word();
        word.setWord(parts[0]);
        word.setPositions(parts[1]);
        return word;
      });

    Dataset<Row> wordDF = spark.createDataFrame(wordRDD, Word.class);
    wordDF.createOrReplaceTempView("words");

    Dataset<Row> resultsDF = spark.sql("SELECT * FROM words WHERE word LIKE CONCAT('%', '" + term + "', '%')");
    Encoder<String> stringEncoder = Encoders.STRING();

    Dataset<String> searchResultsDF = resultsDF.map(
        (MapFunction<Row, String>) row -> "Word: " + row.<String>getAs("word") + " Pos: " + row.<String>getAs("positions"),
        stringEncoder);
    searchResultsDF.show();

    spark.stop();
  }

  public static class Word implements Serializable {
    private String word;
    private String positions;

    public String getWord() {
      return word;
    }

    public String getPositions() {
      return positions;
    }

    public void setWord(String w) {
      word = w;
    }

    public void setPositions(String a) {
      positions = a;
    }
  }
}

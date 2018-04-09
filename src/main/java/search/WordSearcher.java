package search;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.RowFactory;
//import org.apache.spark.sql.SQLContext;
//import org.apache.spark.sql.types.DataTypes;
//import org.apache.spark.sql.types.StructField;
//import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
//import java.util.Collections;
//import java.util.List;

public class WordSearcher {
  public static void main(String[] args) {
    final String KEY_VALUE_DELIMITER = "	";
    final String VALUE_DELIMITER = ";";
    String input = "aaron";

    /*
     * Prepare Spark context
     */
    SparkConf sparkConf = new SparkConf().setAppName("TestApp").setMaster("local[*]");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    JavaRDD<String> file = ctx.textFile("output/part-r-00000");

    /*
     * Sort the input to prepare the key to search for the possible words
     */
    char[] inputArray = input.toCharArray();
    Arrays.sort(inputArray);
    final String sortedInput = String.valueOf(inputArray);

    /*
     * Search the data file for the possible words based on the key
     */
    JavaRDD<String> possibleWords = file.filter(new Function<String, Boolean>() {
      public Boolean call(String s) {
        return s.contains(sortedInput);
      }
    });

    // the result can be cached in-memory for subsequent searches
    // possibleWords.cache();

    /*
     * filter the key out and printout just the possible words
     */
    System.out.println("Possible words:-");
    for (String s:possibleWords.collect()) {
      if (s.contains(sortedInput) && s.contains(KEY_VALUE_DELIMITER)) {
        String[] line = s.split(KEY_VALUE_DELIMITER);
        if (line.length>0) {
          String word = line[1].replace(KEY_VALUE_DELIMITER, "");
          if (!word.isEmpty()) {
            for (String w: word.split(VALUE_DELIMITER)) {
              System.out.println(w);
            }
          }
        }
      }
    }
    System.out.println();

    ctx.stop();
  }
}

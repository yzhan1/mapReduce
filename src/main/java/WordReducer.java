import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Shu Lin Chan, Jonathan Maeda, James Wang, Yaoming Zhan
 * Final Project
 */
public class WordReducer extends Reducer<Text, Text, Text, Text> {
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
    Map<String, ArrayList<Integer>> map = new HashMap<>();
    for (Text t : values) {
      String[] pair = t.toString().split(";");
      String id = pair[1];
      if (!map.containsKey(id)) {
        map.put(id, new ArrayList<>());
      }
      map.get(id).add(Integer.valueOf(pair[0]));
    }

    map.keySet().forEach((k) -> map.get(k).sort(Comparator.comparingInt(a -> a)));

    StringBuilder builder = new StringBuilder();
    for (String k : map.keySet()) {
      builder.append(k).append(".");
      for (Integer i : map.get(k)) {
        builder.append(i).append(".");
      }
      builder.deleteCharAt(builder.length() - 1);
      builder.append(",");
    }
    builder.deleteCharAt(builder.length() - 1);

    ctx.write(key, new Text(builder.toString()));
  }
}

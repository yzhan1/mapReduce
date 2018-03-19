import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Shu Lin Chan, Jonathan Maeda, James Wang, Yaoming Zhan
 * Final Project
 */
public class WordReducer extends Reducer<Text, Text, Text, Text> {
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
    Map<String, String> map = new HashMap<>();
    for (Text t : values) {
      String[] pair = t.toString().split(";");
      String id = pair[0];
      if (map.containsKey(id)) {
        String string = map.get(id) + "." + pair[1];
        map.put(id, string);
      } else {
        map.put(id, pair[1]);
      }
    }

    StringBuilder builder = new StringBuilder();
    for (String k : map.keySet()) {
      builder.append(k).append(".").append(map.get(k)).append(",");
    }
    
    Text output = new Text(builder.toString());
    ctx.write(key, output);
  }
}

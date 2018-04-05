package mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import writable.WikiWritable;

/**
 * @author Shu Lin Chan, Jonathan Maeda, James Wang, Yaoming Zhan
 * Final Project
 */
public class WordReducer extends Reducer<WikiWritable, WikiWritable, Text, Text> {
  @Override
  protected void reduce(WikiWritable key, Iterable<WikiWritable> values, Context ctx) throws IOException, InterruptedException {
    Map<String, StringBuilder> map = new HashMap<>();
    for (WikiWritable w : values) {
      String id = String.valueOf(w.getDocId());
      if (map.containsKey(id)) {
        map.get(id).append(w.getPosition()).append(".");
      } else {
        map.put(id, new StringBuilder());
        map.get(id).append(".").append(w.getPosition()).append(".");
      }
    }

    StringBuilder builder = new StringBuilder();
    map.keySet().forEach((k) -> {
      map.get(k).deleteCharAt(map.get(k).length() - 1);
      builder.append(k).append(map.get(k)).append(";");
    });

    ctx.write(key.getWord(), new Text(builder.toString()));
  }
}

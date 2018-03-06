import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortingReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context ctx)
			throws IOException, InterruptedException {
		List<String> namesToSort = new ArrayList<>();
		for (Text t : values)
			namesToSort.add(t.toString());
		Collections.sort(namesToSort);
		for (String s : namesToSort) {
			Text t = new Text(s);
			ctx.write(t, null);
		}
	}
}

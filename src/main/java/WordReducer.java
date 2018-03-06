import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Shu Lin Chan
 * lab #1
 */
public class WordReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context ctx)
			throws IOException, InterruptedException {
		//Using a set so docIds are not repeated
		Set<String> docIds = new HashSet<>();
		for (Text t : values)
			docIds.add(t.toString());
		
		Text docIdsCommaSeperated = new Text(String.join(",", docIds));

		ctx.write(key, docIdsCommaSeperated);
	}
}

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FirstCharsMapper extends Mapper<LongWritable, Text,
 Text, Text>
{
	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		Text outKey = new Text(value.toString().substring(0, 1));
		context.write(outKey, value);
	}
}
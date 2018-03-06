import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author Shu Lin Chan
 * lab #1
 */
public class Driver {
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    Path wiki0 = new Path(args[0]);
    Path wiki1 = new Path(args[1]);
    Path wiki2 = new Path(args[2]);
    Path out = new Path(args[3]);
    
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "name sorter");
    
    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(job, wiki0);
    TextInputFormat.addInputPath(job, wiki1);
    TextInputFormat.addInputPath(job, wiki2);
    
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, out);
    
    job.setJarByClass(Driver.class);
    job.setMapperClass(WordMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setReducerClass(WordReducer.class);
    
    job.waitForCompletion(true);
  }
}
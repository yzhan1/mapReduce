package drivers;

import java.io.IOException;

import mapreduce.WordMapper;
import mapreduce.WordReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import secondarysort.WikiComparator;
import secondarysort.WikiPartitioner;
import writable.WikiWritable;

/**
 * @author Shu Lin Chan, Jonathan Maeda, James Wang, Yaoming Zhan
 * Final Project
 */
public class Driver {
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    Path wiki = new Path(args[0]);
    Path out = new Path(args[1]);
    
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "cs132g4");

    TextInputFormat.addInputPath(job, wiki);
    TextOutputFormat.setOutputPath(job, out);

    job.setMapOutputKeyClass(WikiWritable.class);
    job.setMapOutputValueClass(WikiWritable.class);

    job.setJarByClass(Driver.class);
    job.setMapperClass(WordMapper.class);
    job.setOutputKeyClass(WikiWritable.class);
    job.setOutputValueClass(WikiWritable.class);
    job.setReducerClass(WordReducer.class);
    job.setPartitionerClass(WikiPartitioner.class);
    job.setGroupingComparatorClass(WikiComparator.class);

    job.waitForCompletion(true);
  }
}
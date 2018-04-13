package secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import writable.WikiWritable;

/**
 * @author Shu Lin Chan, Jonathan Maeda, James Wang, Yaoming Zhan
 * Final Project
 */
public class WikiPartitioner extends Partitioner<WikiWritable, Text> {
  @Override
  public int getPartition(WikiWritable wikiWritable, Text text, int i) {
    return Math.abs(wikiWritable.getWord().hashCode() % i);
  }
}

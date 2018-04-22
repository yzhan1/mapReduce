package secondarysort;

import org.apache.hadoop.mapreduce.Partitioner;
import writable.WikiWritable;

/**
 * @author Shu Lin Chan, Jonathan Maeda, James Wang, Yaoming Zhan
 * Final Project
 */
public class WikiPartitioner extends Partitioner<WikiWritable, WikiWritable> {
    @Override
    public int getPartition(WikiWritable wikiWritable, WikiWritable wikiWritable2, int i) {
        int hash = wikiWritable.getWord().charAt(0) - 'a';
        return Math.abs(hash % i);
//        return getHash(wikiWritable.getWord().toString(), i);
    }

    public static int getHash(String word, int mod) {
        int first = word.charAt(0) - 'a';
        int second = word.charAt(1) - 'a';
        return Math.abs((first * 26 + second) % mod);
    }
}

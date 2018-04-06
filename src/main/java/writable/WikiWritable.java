package writable;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WikiWritable implements Writable, WritableComparable<WikiWritable> {
  private Text word; // natural key

  // secondary keys
  private IntWritable docId;
  private IntWritable position;

  public WikiWritable() {
    this.word = new Text();
    this.docId = new IntWritable();
    this.position = new IntWritable();
  }

  public WikiWritable(String word, String docId, int position) {
    set(word, docId, position);
  }

  private void set(String word, String docId, int position) {
    this.word = new Text(word);
    this.docId = new IntWritable(Integer.valueOf(docId));
    this.position = new IntWritable(position);
  }

  @Override
  public int compareTo(WikiWritable o) {
    int result = this.word.compareTo(o.word);
    result = result == 0 ? this.docId.compareTo(o.docId) : result;
    result = result == 0 ? this.position.compareTo(o.position) : result;
    return result;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    word.write(dataOutput);
    docId.write(dataOutput);
    position.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    word.readFields(dataInput);
    docId.readFields(dataInput);
    position.readFields(dataInput);
  }

  public IntWritable getDocId() {
    return docId;
  }

  public int getPosition() {
    return Integer.valueOf(position.toString());
  }

  public Text getWord() {
    return word;
  }
}

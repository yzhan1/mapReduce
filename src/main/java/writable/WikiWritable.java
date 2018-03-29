package writable;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WikiWritable implements WritableComparable<WikiWritable> {
  private IntWritable docId;
  private IntWritable position;

  public WikiWritable() {
    this.docId = new IntWritable();
    this.position = new IntWritable();
  }

  public void set(String docId, int position) {
    this.docId = new IntWritable(Integer.valueOf(docId));
    this.position = new IntWritable(position);
  }

  @Override
  public int compareTo(WikiWritable o) {
    int result = this.docId.compareTo(o.docId);
    result = result == 0 ? this.position.compareTo(o.position) : result;
    return result;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    docId.write(dataOutput);
    position.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    docId.readFields(dataInput);
    position.readFields(dataInput);
  }

  public IntWritable getDocId() {
    return docId;
  }

  public int getPosition() {
    return Integer.valueOf(position.toString());
  }
}

package eps.examples.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.codehaus.jettison.json.JSONObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SentimentWritable implements Writable {

    // Some data
    private IntWritable sentiment;
    private IntWritable length;

    // Default constructor to allow (de)serialization
    SentimentWritable() {
        this.sentiment = new IntWritable();
        this.length = new IntWritable();
    }

    public SentimentWritable(IntWritable sentiment, IntWritable length) {
        this.sentiment = sentiment;
        this.length = length;
    }

    public IntWritable getSentiment() {
        return sentiment;
    }

    public void setSentiment(IntWritable sentiment) {
        this.sentiment = sentiment;
    }

    public IntWritable getLength() {
        return length;
    }

    public void setLength(IntWritable length) {
        this.length = length;
    }

    public void write(DataOutput out) throws IOException {
        sentiment.write(out);
        length.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        sentiment.readFields(in);
        length.readFields(in);
    }
}

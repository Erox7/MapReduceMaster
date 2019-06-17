package eps.examples.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TweetWritable implements Writable {

    // Some data
    private Text hashtagh;
    private Text tweetJson;

    // Default constructor to allow (de)serialization
    TweetWritable() {
        this.hashtagh = new Text();
        this.tweetJson = new Text();
    }

    public TweetWritable(Text hashtagh, Text tweetJson) {
        this.hashtagh = hashtagh;
        this.tweetJson = tweetJson;
    }

    public Text getHashtagh() {
        return hashtagh;
    }

    public void setHashtagh(Text hashtagh) {
        this.hashtagh = hashtagh;
    }

    public Text getTweetJson() {
        return tweetJson;
    }

    public void setTweetJson(Text tweetJson) {
        this.tweetJson = tweetJson;
    }
    public void write(DataOutput out) throws IOException {
        hashtagh.write(out);
        tweetJson.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        hashtagh.readFields(in);
        tweetJson.readFields(in);
    }
}

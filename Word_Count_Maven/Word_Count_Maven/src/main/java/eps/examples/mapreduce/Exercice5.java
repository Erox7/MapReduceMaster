package eps.examples.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Nando on 27/3/19.
 */
// hadoop jar WordCount-1.0.jar eps.examples.mapreduce.Exercice5 /shared/nando/data/tweets output positive-words.txt negative-words.txt
public class Exercice5 extends Configured implements Tool
{
    public static class HashtagSeparatorMapper
            extends Mapper<Object, Text, TweetWritable, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private TweetWritable tweetInfo = new TweetWritable();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            try {
                JSONObject json = new JSONObject(value.toString());
                String text = json.getString("text");
                Pattern p = Pattern.compile("(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)");
                Matcher m;
                String[] words = text.split("[,.-_@/ \n]") ;
                for (String str: words)
                {
                    m = p.matcher(str);
                    if(m.find() && !str.equals("") && !value.toString().equals("")) {
                        tweetInfo.setHashtagh(new Text(str));
                        tweetInfo.setTweetJson(new Text(value.toString()));
                        context.write(tweetInfo, one);  //Write map result {word,1}
                    }
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }

    public static class FilterJsonMapper
            extends Mapper<TweetWritable, IntWritable, TweetWritable, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);

        public void map(TweetWritable key, IntWritable value, Context context
        ) throws IOException, InterruptedException {
            try {

                JSONObject json = new JSONObject(key.getTweetJson().toString());
                String lang = json.getString("lang");
                if (lang.equals("en")){
                    key.setTweetJson(new Text(key.getTweetJson().toString().toLowerCase()));
                    context.write(key,value);
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }

    public static class OverallSentimentMapper
            extends Mapper<TweetWritable, IntWritable, Text, SentimentWritable>
    {
        private Set badWords = new HashSet();
        private Set goodWords = new HashSet();
        private Text hashtagText = new Text();
        private SentimentWritable sw = new SentimentWritable();

        private Integer sentiment = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try{
                Path[] stopWordsFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                if(stopWordsFiles != null && stopWordsFiles.length > 0) {
                    for(Path stopWordFile : stopWordsFiles) {
                        readFile(stopWordFile);
                    }
                }
            } catch(IOException ex) {
                System.err.println("Exception in mapper setup: " + ex.getMessage());
            }
        }

        public void map(TweetWritable key, IntWritable value, Context context
        ) throws IOException, InterruptedException {
            try {

                JSONObject tweet = new JSONObject(key.getTweetJson().toString());
                String text = tweet.getString("text");
                StringTokenizer st = new StringTokenizer(text," ,'.\\/");

                while(st.hasMoreTokens()){
                    String wordText = st.nextToken();
                    if(badWords.contains(wordText.toLowerCase())) {
                        sentiment--;
                    }else if(goodWords.contains(wordText.toLowerCase())){
                        sentiment++;
                    }
                }

                sw.setLength(new IntWritable(text.length()));
                sw.setSentiment(new IntWritable(sentiment));
                hashtagText.set(key.getHashtagh());
                context.write(hashtagText,sw);
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

        private void readFile(Path filePath) {
            try{
                BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
                String stopWord = null;
                while((stopWord = bufferedReader.readLine()) != null) {
                    if(filePath.toString().contains("positive")){
                        goodWords.add(stopWord.toLowerCase());
                    }else{
                        badWords.add(stopWord.toLowerCase());
                    }
                }
            } catch(IOException ex) {
                System.err.println("Exception while reading stop words file: " + ex.getMessage());
            }
        }


    }

    public static class OverallSentimentReducer
            extends Reducer<Text,SentimentWritable,Text,IntWritable>
    {
        private IntWritable overallSentimentWritable = new IntWritable();
        public void reduce(Text key, Iterable<SentimentWritable> values,
                           Context context) throws IOException, InterruptedException
        {

            Integer totalLength = 1;
            Integer totalSentiment = 1;
            for(SentimentWritable sw : values) {
                totalLength += Integer.parseInt(sw.getLength().toString());
                totalSentiment += Integer.parseInt(sw.getSentiment().toString());
            }
            Integer overallSentiment = totalSentiment / totalLength;
            overallSentimentWritable.set(overallSentiment);
            context.write(key, overallSentimentWritable);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        String positiveWordsPath = args[2];
        conf.set("positiveWordsPath",positiveWordsPath);
        String negativeWordsPath = args[3];
        conf.set("negativeWordsPath",negativeWordsPath);

        Job job = Job.getInstance(conf, "Exercice 5");

        job.setJarByClass(Exercice5.class);

        DistributedCache.addCacheFile(new URI(positiveWordsPath), job.getConfiguration());
        DistributedCache.addCacheFile(new URI(negativeWordsPath), job.getConfiguration());

        ChainMapper.addMapper(job, HashtagSeparatorMapper.class, Object.class, Text.class, TweetWritable.class, IntWritable.class, new Configuration(false));
        ChainMapper.addMapper(job, FilterJsonMapper.class, TweetWritable.class, IntWritable.class, TweetWritable.class, IntWritable.class, new Configuration(false));
        ChainMapper.addMapper(job, OverallSentimentMapper.class, TweetWritable.class, IntWritable.class, Text.class, SentimentWritable.class, new Configuration(false));
        ChainReducer.setReducer(job,OverallSentimentReducer.class, Text.class, SentimentWritable.class,Text.class,IntWritable.class, new Configuration(false));
        //job.setReducerClass(OverallSentimentReducer.class);
        //job.setNumReduceTasks(1);

        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Exercice5(), args);
        System.exit(exitCode);
    }
}


package eps.examples.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONArray;

import java.io.IOException;

/**
 * Created by Nando on 27/3/19.
 */
// hadoop jar WordCount-1.0.jar eps.examples.mapreduce.Exercice2 /shared/nando/data/tweets output
public class Exercice2 extends Configured implements Tool
{
    public static class FilterFieldsMapper
            extends Mapper<Object, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            try {

                JSONObject json = new JSONObject(value.toString());
                JSONObject customJson = new JSONObject();
                String text = json.getString("text");
                String lang = json.getString("lang");

                JSONObject entities = json.getJSONObject("entities");
                JSONArray hashtags = entities.getJSONArray("hashtags");

                customJson.put("text",text);
                customJson.put("hashtags", hashtags);
                customJson.put("lang", lang);

                if(!text.equals("") && hashtags.length() > 0 && (lang.equals("es") || lang.equals("en"))){
                    word.set(customJson.toString().toLowerCase());
                    context.write(word, one);  //Write map result {word,1}
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }


    public static class CleanFieldsMapper
            extends Mapper<Text, IntWritable, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            try {

                JSONObject json = new JSONObject(key.toString());
                String text = json.getString("text");
                text.toLowerCase();
                json.put("text", text);
                word.set(json.toString().toLowerCase());
                context.write(word, one);  //Write map result {word,1}
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }


    public static class WordCountReducer
            extends Reducer<Text,IntWritable,Text,IntWritable>
    {
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException
        {
            int total = 0;
            for (IntWritable val : values) {
                total++ ;
            }
            context.write(key, new IntWritable(total));  // Write reduce result {word,count}
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "Exercice 2");
        job.setJarByClass(Exercice2.class);

        ChainMapper.addMapper(job, FilterFieldsMapper.class, Object.class, Text.class, Text.class, IntWritable.class, new Configuration(false));
        ChainMapper.addMapper(job, CleanFieldsMapper.class, Text.class, IntWritable.class, Text.class, IntWritable.class, new Configuration(false));

        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Exercice2(), args);
        System.exit(exitCode);
    }
}


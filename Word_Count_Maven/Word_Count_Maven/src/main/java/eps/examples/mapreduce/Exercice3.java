package eps.examples.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Nando on 27/3/19.
 */
// hadoop jar WordCount-1.0.jar eps.examples.mapreduce.Exercice3 /shared/nando/data/tweets output N
public class Exercice3 extends Configured implements Tool
{
    public static class HashtagSeparatorMapper
            extends Mapper<Object, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
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
                    if(m.find()) {
                        word.set(str);
                        context.write(word, one);  //Write map result {word,1}
                    }
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }

    public static class TopNMapper
            extends Mapper<Text, IntWritable, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private TreeMap<String,Integer> top2N = new TreeMap<String,Integer>();
        private Integer N;
        private IntWritable keyValue;

        public void map(Text key, IntWritable value, Context context
        ) throws IOException, InterruptedException {
            N = Integer.parseInt(context.getConfiguration().get("N"));
            if(top2N.containsKey(key.toString())){
                top2N.put(key.toString(), top2N.get(key.toString()) + Integer.parseInt(value.toString()));
            } else {
                top2N.put(key.toString(), Integer.parseInt(value.toString()));
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            Map<String,Integer> sortedMap = sortMap(top2N, N*2);
            for(Map.Entry<String,Integer> pair : sortedMap.entrySet()){
                keyValue = new IntWritable(pair.getValue());
                word.set(pair.getKey());
                context.write(word,keyValue);
            }
        }

        public Map<String , Integer > sortMap (Map<String,Integer> hashtagsMap, int N){

            Map<String ,Integer> hashmap = new HashMap<String,Integer>();
            int count=0;
            List<Map.Entry<String,Integer>> list = new LinkedList<Map.Entry<String,Integer>>(hashtagsMap.entrySet());
            Collections.sort(list , new Comparator<Map.Entry<String,Integer>>(){
                public int compare (Map.Entry<String , Integer> o1 , Map.Entry<String , Integer> o2 ) {
                    //sorting in descending order
                    return o2.getValue().compareTo(o1.getValue());
                }
            });
            for(Map.Entry<String, Integer> entry : list){
                if(count>N)
                    break;
                hashmap.put(entry.getKey(),entry.getValue());
                count++;
            }
            return hashmap ;
        }

    }

    public static class TopNReducer
            extends Reducer<Text,IntWritable,Text,IntWritable>
    {
        private TreeMap<String,Integer> topNtotal = new TreeMap<String,Integer>();
        private Integer N;
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException
        {
            N = Integer.parseInt(context.getConfiguration().get("N"));
            int total = 0;
            for (IntWritable val : values) {
                total+= val.get();
            }
            topNtotal.put(key.toString(), total);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            Map<String,Integer> topN = sortMap(topNtotal, N);

            for(Map.Entry<String,Integer> pair : topN.entrySet()){
                context.write(new Text(pair.getKey()),new IntWritable(pair.getValue()));
            }
        }

        public Map<String , Integer > sortMap (Map<String,Integer> hashtagsMap, int N){

            Map<String ,Integer> hashmap = new HashMap<String,Integer>();
            int count=0;
            List<Map.Entry<String,Integer>> list = new LinkedList<Map.Entry<String,Integer>>(hashtagsMap.entrySet());
            Collections.sort(list , new Comparator<Map.Entry<String,Integer>>(){
                public int compare (Map.Entry<String , Integer> o1 , Map.Entry<String , Integer> o2 ) {
                    //sorting in descending order
                    return o2.getValue().compareTo(o1.getValue());
                }
            });
            for(Map.Entry<String, Integer> entry : list){
                if(count>N)
                    break;
                hashmap.put(entry.getKey(),entry.getValue());
                count++;
            }
            return hashmap ;
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        String N = args[2];
        conf.set("N",N);
        System.out.println(N);
        Job job = Job.getInstance(conf, "Exercice 3");

        job.setJarByClass(Exercice3.class);
        ChainMapper.addMapper(job, HashtagSeparatorMapper.class, Object.class, Text.class, Text.class, IntWritable.class, new Configuration(false));
        ChainMapper.addMapper(job, TopNMapper.class, Text.class, IntWritable.class, Text.class, IntWritable.class, new Configuration(false));

        job.setReducerClass(TopNReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Exercice1(), args);
        System.exit(exitCode);
    }
}


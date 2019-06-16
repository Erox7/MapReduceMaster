package eps.examples.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;

/**
 * Created by Nando on 27/3/19.
 */
// hadoop jar WordCount-1.0.jar eps.examples.mapreduce.Exercice4 /shared/nando/data/tweets output 5
public class Exercice4 extends Configured implements Tool
{
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        String N = args[2];
        conf.set("N",N);

        Job jobEx2 = Job.getInstance(conf, "Exercice 2");
        jobEx2.setJarByClass(Exercice2.class);

        ChainMapper.addMapper(jobEx2, Exercice2.FilterFieldsMapper.class, Object.class, Text.class, Text.class, IntWritable.class, new Configuration(false));
        ChainMapper.addMapper(jobEx2, Exercice2.CleanFieldsMapper.class, Text.class, IntWritable.class, Text.class, IntWritable.class, new Configuration(false));

        jobEx2.setReducerClass(Exercice2.WordCountReducer.class);
        jobEx2.setOutputKeyClass(Text.class);
        jobEx2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(jobEx2, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobEx2, new Path("./test"));

        ControlledJob cJobEx2 = new ControlledJob(conf);
        cJobEx2.setJob(jobEx2);



        Job jobEx3 = Job.getInstance(conf, "Exercice 3");

        jobEx3.setJarByClass(Exercice3.class);
        ChainMapper.addMapper(jobEx3, Exercice3.HashtagSeparatorMapper.class, Object.class, Text.class, Text.class, IntWritable.class, new Configuration(false));
        ChainMapper.addMapper(jobEx3, Exercice3.TopNMapper.class, Text.class, IntWritable.class, Text.class, IntWritable.class, new Configuration(false));

        jobEx3.setReducerClass(Exercice3.TopNReducer.class);
        jobEx3.setNumReduceTasks(1);

        jobEx3.setOutputKeyClass(Text.class);
        jobEx3.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(jobEx3, new Path("./test"));
        FileOutputFormat.setOutputPath(jobEx3, new Path(args[1]));

        ControlledJob cJobEx3 = new ControlledJob(conf);
        cJobEx3.setJob(jobEx3);


        JobControl jobCtrlEx4 = new JobControl("Exercice 4");
        jobCtrlEx4.addJob(cJobEx2);
        jobCtrlEx4.addJob(cJobEx3);
        cJobEx3.addDependingJob(cJobEx2);

        Thread jobRunnerThread = new Thread(new JobRunner(jobCtrlEx4));
        jobRunnerThread.start();
        while (!jobCtrlEx4.allFinished()) {
            System.out.println("Still running...");
            Thread.sleep(5000);
        }
        System.out.println("done");
        jobCtrlEx4.stop();
        // Cleaning intermediate data.. can be ignored.
        FileSystem fs = new RawLocalFileSystem();
        fs.delete(new Path("./test"), true);
        fs.close();

        try {
            File file = new File("./test");
            boolean sas = file.delete();
        }catch(Exception e){

        }

        return 1;
    }

    class JobRunner implements Runnable {
        private JobControl control;
        public JobRunner(JobControl _control) {
            this.control = _control;
        }
        public void run() {
            this.control.run();
        }
    }
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Exercice4(), args);
        System.exit(exitCode);
    }
}


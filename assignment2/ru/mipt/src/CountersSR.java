package ru.mipt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CountersSR extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new CountersSR(), args);
    }

    static enum IntCount { DISPARITY }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = new Job(conf);
        job.setJarByClass(CountersSR.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Mapper1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapperForUsers.class);

        Path tmp = new Path(args[2]+"-temp");
        SequenceFileOutputFormat.setOutputPath(job, tmp);

        if (!job.waitForCompletion(true)) {
            return -1;
        }

        Job job1 = new Job(conf);
        job1.setJarByClass(CountersSR.class);
        job1.setMapperClass(Mapper2.class);
        job1.setReducerClass(Reducer2.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setNumReduceTasks(1);

        SequenceFileInputFormat.addInputPath(job1, tmp);

        Path out = new Path(args[2]);
        FileOutputFormat.setOutputPath(job1, out);
        if(job1.waitForCompletion(true)){
            FileSystem hdfs = FileSystem.get(conf);
            if (hdfs.exists(tmp)) {
                hdfs.delete(tmp, true);
            }
            System.err.println("My counter is:  " + job1.getCounters().findCounter(IntCount.DISPARITY).getValue());
            return 0;
        } else {
            return -1;
        }

    }

}

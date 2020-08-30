package com.company;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.DataInput;
import java.io.DataOutput;
import java.lang.InterruptedException;
import java.io.IOException;
import java.util.ArrayList;

public class PopularTags extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new PopularTags(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = new Job(conf);
        job.setJarByClass(PopularTags.class);
        job.setMapperClass(PopularTagsMapper.class);
        job.setReducerClass(PopularTagsReducer.class); // = IntSumReducer.class

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(8);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : -1;
    }



}

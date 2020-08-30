package com.company;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PopularTagsReducer extends Reducer<TextPair, IntWritable, TextPair, IntWritable> {
    @Override
    protected void reduce(TextPair uid, Iterable<IntWritable> values, Context context)
            throws IOException,InterruptedException
    {
        int sum = 0;
        for (IntWritable value: values) {
            sum += value.get();
        }

        context.write(uid, new IntWritable(sum));
    }
}

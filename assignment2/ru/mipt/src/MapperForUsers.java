package ru.mipt;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


public class MapperForUsers extends Mapper<LongWritable, Text, Text, Text> {
    private static Text one = new Text();
    private Text uid = new Text();

    @Override
    public void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {
        String[] fields1 = line.toString().split("\\s+");
        for (int j = 0; j < fields1.length; j++) {
            if (fields1[j].startsWith("Id=\"")) {
                fields1[j] = fields1[j].substring(4, fields1[j].length() - 1);
                uid = new Text(fields1[j]);
            }
            if (fields1[j].startsWith("Reputation=\"")) {
                fields1[j] = fields1[j].substring(12, fields1[j].length() - 1);
                one = new Text("rep"+fields1[j]);
            }
        }
        context.write(uid, one);
    }
}


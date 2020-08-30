package ru.mipt;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


public class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
    private static Text one = new Text();
    private Text uid = new Text();

    @Override
    public void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {
        String[] fields1 = line.toString().split("\\s+");
        for (int i = 0; i < fields1.length; i++) {
            if (fields1[i].contains("parId")) {
                uid = new Text(fields1[i]);
                break;
            }

        }
        String str = "";
        for (int i = 0; i < fields1.length; i++) {
            if (!fields1[i].contains("parId")) {
                str += fields1[i] + "\t";
            }
        }
        if (str.length()>0) {
            context.write(uid, new Text(str));
        }
    }
}



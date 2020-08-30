package ru.mipt;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.ArrayList;


public class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
    private static Text one = new Text();
    private Text uid = new Text();

    @Override
    public void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {
        String lline = line.toString().trim();
        String[] fields1 = lline.split("\\s+");
        if (fields1[0].contains("row")) {
            for (int i = 0; i < fields1.length; i++) {
                if (isAnswer(fields1[i])) {
                    for (int ii = i; ii < fields1.length; ii++) {
                        if (isOwnerUserId(fields1[ii])) {
                            uid = new Text(fields1[ii].substring(13, fields1[ii].length() - 1));
                            ArrayList<String> s = getOne(fields1);
                            if (s.size() > 0) {
                                context.write(uid, new Text("parId" + s.get(0) + "\t" + "Score" + s.get(1) + "\t"));
                            }
                        }
                    }
                }
            }
        }
    }

    public boolean isOwnerUserId(String tmp) {
        boolean result = false;
        if (tmp.length() >= 15) {
            if (tmp.contains("OwnerUserId")) {
                result = true;
            }
        }
        return result;
    }

    public boolean isAnswer(String tmp) {
        boolean result = false;
        if (tmp.length() >= 14) {
            if (tmp.contains("PostTypeId")) {
                if (("2").equals(tmp.substring(12, 13))) {
                    result = true;
                }
            }
        }
        return result;
    }

    public ArrayList<String> getOne(String[] fields){
        ArrayList<String> str = new ArrayList<String>();
        for (int j = 0; j < fields.length; j++) {
            if(fields[j].length()>=12) {
                if (fields[j].contains("ParentId=\"")) {
                    fields[j] = fields[j].substring(10, fields[j].length() - 1);
                    str.add(fields[j]);
                    break;
                }
            }
        }
        for (int j = 0; j < fields.length; j++) {
            if(fields[j].length()>=9) {
                if (fields[j].startsWith("Score=\"")) {
                    fields[j] = fields[j].substring(7, fields[j].length() - 1);
                    str.add(fields[j]);
                    break;
                }
            }
        }
        return str;

    }
}

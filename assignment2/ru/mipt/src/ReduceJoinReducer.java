package ru.mipt;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;

public class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text uid, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String out = "";
        String repp = "";
        ArrayList<String> list = new ArrayList<String>();
        for (Text value : values) {
            if (value.toString().contains("rep")) {
                    repp = value.toString();
            }
            String[] str = value.toString().split("\\s+");
            for (int i = 0; i<str.length;i++) {
                if (str[i].contains("parId")) {
                    list.add(str[i]);
                }
                if (str[i].contains("Score")) {
                    list.add(str[i]);
                }
            }
        }
        if (list.size()>0) {
            for (int i = 0; i < list.size() - 1; i = i + 2) {
                if (repp.length()>0){
                    out = list.get(i) + "\t" + list.get(i + 1) + "\t" + repp;
                    context.write(uid, new Text(out));
                }
            }
        }
    }
}

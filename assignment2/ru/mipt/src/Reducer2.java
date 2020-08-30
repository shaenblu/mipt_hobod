package ru.mipt;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class Reducer2 extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text uid, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        ArrayList<String> list = new ArrayList<String>();
        ArrayList<String> score = new ArrayList<String>();
        ArrayList<String> rep = new ArrayList<String>();
        for (Text value : values) {
            String[] str = value.toString().split("\\s+");
            if (value.toString().length()>0){
                list.add(value.toString());
            }
            for (int i = 0; i<str.length;i++) {
                if (str[i].startsWith("Score")) {
                    score.add(str[i]);
                }
                if (str[i].startsWith("rep")) {
                    rep.add(str[i]);
                }
            }
        }

        Collections.sort(score, Collections.reverseOrder());
        Collections.sort(rep, Collections.reverseOrder());

        String max_score = "";
        String max_rep = "";
        if(score.size()>0){
            max_score = score.get(0);
        }

        if(rep.size()>0) {
            max_rep = rep.get(0);
        }
        String id_s = "";
        String id_r = "";

        for (int i = 0; i < list.size(); i ++) {
            String[] str = list.get(i).split("\\s+");
            for (int j = 0; j < str.length; j++) {
                if (str[j].equals(max_score)) {
                    for (int p = 0; p < str.length; p++) {
                        if(str[p].length()>0){
                            if (!str[p].startsWith("Score") && !str[p].startsWith("rep")) {
                                id_s = str[p];
                            }
                        }
                    }
                }
                if (str[j].equals(max_rep)) {
                    for (int p = 0; p < str.length; p++) {
                        if (str[p].length()>0){
                            if (!str[p].startsWith("Score") && !str[p].startsWith("rep")) {
                                id_r = str[p];
                            }
                        }
                    }
                }
            }
        }
        if (!id_s.equals(id_r)){
            context.getCounter(CountersSR.IntCount.DISPARITY).increment(1);
            String str = uid.toString().substring(5);
            context.write(new Text(str), new Text());
        }
    }
}

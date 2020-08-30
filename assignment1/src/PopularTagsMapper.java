package com.company;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class PopularTagsMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {
    private static IntWritable one = new IntWritable(1);
    private TextPair uid = new TextPair();

    @Override
    public void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {
        String[] fields1 = line.toString().split("\\<");

        //int k_10 = 0;
        //int k_16 = 0;


        if (fields1[1].startsWith("row")) {
            String[] fields = fields1[1].split("\\s");

            for (int j = 1; j < fields.length; j++) {
                if (fields[j].length() > 18) {
                    if (fields[j].startsWith("CreationDate")) {
                        if (fields[j].substring(14, 18).equals("2010")) {
                            //k_10++;
                            ArrayList<String> tags_10 = getTags(fields);
                            if (tags_10.size() != 0) {
                                String[] ff = new String[tags_10.size()];
                                for (int p = 0; p < tags_10.size(); p++) {
                                    ff[p] = tags_10.get(p);
                                    uid.set(new Text(ff[p]), new Text("2010"));
                                    context.write(uid, one);
                                }
                            }
                        }
                    }
                    if (fields[j].startsWith("CreationDate")) {
                        if (fields[j].substring(14, 18).equals("2016")) {
                            //System.out.println("2016");
                            //k_16++;
                            ArrayList<String> tags_16 = getTags(fields);
                            if (tags_16.size() != 0) {
                                String[] ff = new String[tags_16.size()];
                                for (int p = 0; p < tags_16.size(); p++) {
                                    ff[p] = tags_16.get(p);
                                    uid.set(new Text(ff[p]), new Text("2016"));
                                    context.write(uid, one);

                                }
                            }
                        }
                    }
                }

            }
        }
    }

    public ArrayList<String> getTags(String[] strings) {

        ArrayList<String> words = new ArrayList<String>();
        for (int i = 0; i < strings.length; i++) {
            if (strings[i].length() > 8) {
                if (strings[i].startsWith("Tags")) {
                    strings[i] = strings[i].replaceAll("(\\&amp\\;){1,}", "\\&");

                    strings[i] = strings[i].replaceAll("(\\&gt\\;|\\&lt\\;){1,}", " ");
                    //System.out.println(strings[i]);
                    //strings[i] = strings[i].replaceAll("(\\&lt;)", "");
                    //System.out.println(strings[i]);
                    int len = strings[i].length();
                    strings[i] = strings[i].substring(6, len - 1);

                    String[] mid_words = strings[i].split("\\s");
                    for (int t = 1; t < mid_words.length; t++)
                        words.add(mid_words[t]);
                }

            }
        }
        return words;
    }
}

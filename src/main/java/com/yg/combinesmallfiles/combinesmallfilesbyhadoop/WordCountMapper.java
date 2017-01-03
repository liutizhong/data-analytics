package com.yg.combinesmallfiles.combinesmallfilesbyhadoop;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import com.yg.util.PairOfStringLong;
/**
 * Created by liuti on 2017/1/3.
 */
public class WordCountMapper extends
        Mapper<PairOfStringLong, Text, Text, IntWritable> {

    final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(PairOfStringLong key,
                    Text value,
                    Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] tokens = StringUtils.split(line, " ");
        for (String tok : tokens) {
            word.set(tok);
            context.write(word, one);
        }
    }
}
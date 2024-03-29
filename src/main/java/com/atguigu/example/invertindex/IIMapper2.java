package com.atguigu.example.invertindex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author LHF
 * @date 2021/9/7 20:39
 */
public class IIMapper2 extends Mapper<LongWritable, Text, Text, Text> {
    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("--");
        k.set(split[0]);
        String[] fields = split[1].split("\t");
        v.set(fields[0] + "-->" + fields[1]);
        context.write(k, v);
    }
}

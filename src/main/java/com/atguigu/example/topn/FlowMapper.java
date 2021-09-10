package com.atguigu.example.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author LHF
 * @date 2021/9/6 20:39
 */
public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private FlowBean flow = new FlowBean();
    private Text phone = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println(value.toString());
        String[] fields = value.toString().split("\t");
        phone.set(fields[0]);

        flow.set(Long.parseLong(fields[1]), Long.parseLong(fields[2]));

        context.write(flow, phone);
    }
}

package com.atguigu.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author LHF
 * @date 2020/12/17 8:32
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text phone = new Text();
    private FlowBean flow = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //用制表符切分一行的数据，存到数组中
        String[] fields = value.toString().split("\t");

        //数组中下标为1的部分是手机号
        phone.set(fields[1]);

        //获取数组中的上行流量和下行流量
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        //把上行流量和下行流量set进flow对象
        flow.set(upFlow, downFlow);

        //把输出数据交给Reducer
        context.write(phone, flow);
    }
}

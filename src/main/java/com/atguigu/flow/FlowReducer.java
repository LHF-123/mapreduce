package com.atguigu.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author LHF
 * @date 2020/12/17 8:32
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean sumFlow = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0;
        long sumDownFlow = 0;

        //key是手机号，遍历FlowBean对上行和下行流量做累加
        for (FlowBean value : values) {
            //总上行流量
            sumUpFlow += value.getUpFlow();
            //总下行流量
            sumDownFlow += value.getDownFlow();
        }

        //调用set方法，计算中总流量
        sumFlow.set(sumUpFlow, sumDownFlow);

        //输出数据
        context.write(key, sumFlow);
    }
}

package com.atguigu.example.topn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author LHF
 * @date 2021/9/6 20:39
 */
public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        for (int i = 0; i < 10; i++) {
            if(iterator.hasNext()){
                context.write(iterator.next(),key);
            }
        }

    }
}

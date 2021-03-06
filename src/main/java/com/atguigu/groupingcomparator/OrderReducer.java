package com.atguigu.groupingcomparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author LHF
 * @date 2020/12/20 22:50
 */
public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
//        context.write(key, NullWritable.get());

        Iterator<NullWritable> iterator = values.iterator();

        //输出同一订单的最贵的两个
        for (int i = 0; i < 2; i++) {
            if (iterator.hasNext()){
                context.write(key, iterator.next());
            }
        }
    }
}

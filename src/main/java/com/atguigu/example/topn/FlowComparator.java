package com.atguigu.example.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author LHF
 * @date 2021/9/6 21:49
 * 让所有的数据分到同一组
 */
public class FlowComparator extends WritableComparator {

    protected FlowComparator(){
        super(FlowBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return 0;
    }
}

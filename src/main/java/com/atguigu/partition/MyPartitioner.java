package com.atguigu.partition;

import com.atguigu.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author LHF
 * @date 2020/12/20 21:32
 *
 * 分区顺序从0开始，使用的分区个数不能超出设置的分区个数
 */
public class MyPartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String phone = text.toString();

        //根据手机号分为了5个区
        switch (phone.substring(0, 3)){
            case "136":
                return 0;
            case "137":
                return 1;
            case "138":
                return 2;
            case "139":
                return 3;
            default:
                return 4;
        }
    }
}

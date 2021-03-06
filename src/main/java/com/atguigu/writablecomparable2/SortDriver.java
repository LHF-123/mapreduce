package com.atguigu.writablecomparable2;

import com.atguigu.writablecomparable.FlowBean;
import com.atguigu.writablecomparable.SortMapper;
import com.atguigu.writablecomparable.SortReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author LHF
 * @date 2020/12/20 21:43
 */
public class SortDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(SortDriver.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(MyPartitioner2.class);
        job.setNumReduceTasks(5);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path("F:\\hadoopExer\\PhoneExer\\output"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\hadoopExer\\PhoneExer\\output2"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }

}

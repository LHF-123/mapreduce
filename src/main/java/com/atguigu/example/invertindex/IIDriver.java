package com.atguigu.example.invertindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author LHF
 * @date 2021/9/7 20:38
 */
public class IIDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job1 = Job.getInstance(new Configuration());

        job1.setJarByClass(IIDriver.class);

        job1.setMapperClass(IIMapper1.class);
        job1.setReducerClass(IIReducer1.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job1, new Path("F:\\hadoopExer\\InvertIndex\\input"));
        FileOutputFormat.setOutputPath(job1, new Path("F:\\hadoopExer\\InvertIndex\\output"));

        boolean b = job1.waitForCompletion(true);
        if (b){
            Job job2 = Job.getInstance(new Configuration());

            job2.setJarByClass(IIDriver.class);

            job2.setMapperClass(IIMapper2.class);
            job2.setReducerClass(IIReducer2.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job2, new Path("F:\\hadoopExer\\InvertIndex\\output"));
            FileOutputFormat.setOutputPath(job2, new Path("F:\\hadoopExer\\InvertIndex\\output2"));

            boolean b1 = job2.waitForCompletion(true);
            System.exit(b1 ? 0 : 1);
        }
    }
}

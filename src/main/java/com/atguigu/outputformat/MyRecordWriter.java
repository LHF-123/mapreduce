package com.atguigu.outputformat;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author LHF
 * @date 2020/12/22 23:12
 */
public class MyRecordWriter extends RecordWriter<LongWritable, Text> {

    private FileOutputStream atguigu;
    private FileOutputStream other;

    /**
     * @Description 初始化方法
     */
    public void initalize() throws FileNotFoundException {
        atguigu = new FileOutputStream("F:\\hadoopExer\\OutputFormat\\output\\atguigu.log");
        other = new FileOutputStream("F:\\hadoopExer\\OutputFormat\\output\\other.log");
    }

    /**
     * @Description 将KV值写出，每对KV调用一次
     * @param null
     * @throws IOException
     * @return void
     */
    @Override
    public void write(LongWritable key, Text value) throws IOException, InterruptedException {
        String out = value.toString() + "\n";
        if (out.contains("atguigu")){
            atguigu.write(out.getBytes());
        } else {
            other.write(out.getBytes());
        }
    }

    /**
     * @Description 关闭资源
     * @param null
     * @throws IOException
     * @return void
     */
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(atguigu);
        IOUtils.closeStream(other);
    }
}

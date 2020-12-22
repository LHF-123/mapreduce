package com.atguigu.inputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author LHF
 * @date 2020/12/17 10:03
 *
 * 自定义一个RecordReader，处理一个文件，把这个文件字节读成一个KV值
 */
public class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {

    private boolean notRead = true;

    private Text key = new Text();
    private BytesWritable value = new BytesWritable();

    private FSDataInputStream inputStream;
    private FileSplit fs;

    @Override
    /**
     * @Description 初始化方法，框架会在开始的时候调用一次
     * @param split
     * @param context
     * @throws IOException
     * @return void
     */
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        //转换切片类型到文件切片
        fs = (FileSplit) split;

        //通过切片获取路径
        Path path = fs.getPath();

        //通过路径获取文件系统
        FileSystem fileSystem = path.getFileSystem(context.getConfiguration());

        //开流
        inputStream = fileSystem.open(path);
    }

    @Override
    /**
     * @Description 读取下一组KV值
     * @throws IOException
     * @return boolean 如果读到了，返回true。读完了，返回false
     */
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (notRead){
            //具体读文件的过程
            //读Key
            key.set(fs.getPath().toString());

            //读Value
            byte[] buf = new byte[(int) fs.getLength()];
            inputStream.read(buf);
            value.set(buf, 0, buf.length);

            notRead = false;
            return true;
        }else {
            return false;
        }
    }

    @Override
    /**
     * @Description 获取当前读到的Key
     * @throws IOException
     * @return org.apache.hadoop.io.Text  当前key
     */
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    /**
     * @Description 获取当前读到的Value
     * @throws IOException
     * @return org.apache.hadoop.io.BytesWritable 当前Value
     */
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    /**
     * @Description 当前数据读取的进度
     * @throws IOException
     * @return float
     */
    public float getProgress() throws IOException, InterruptedException {
        return notRead ? 0 : 1;
    }

    @Override
    /**
     * @Description 关闭资源的
     * @throws IOException
     * @return void
     */
    public void close() throws IOException {
        IOUtils.closeStream(inputStream);
    }
}

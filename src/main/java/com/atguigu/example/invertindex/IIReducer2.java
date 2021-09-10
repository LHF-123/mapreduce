package com.atguigu.example.invertindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author LHF
 * @date 2021/9/7 20:40
 */
public class IIReducer2 extends Reducer<Text, Text, Text, Text> {

    private Text v = new Text();

    private StringBuilder stringBuilder = new StringBuilder();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        stringBuilder.delete(0, stringBuilder.length());
        for (Text value : values) {
            stringBuilder.append(value.toString()).append(" ");
        }
        v.set(stringBuilder.toString());
        context.write(key, v);
    }
}

package com.mystudy;

import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//实验一public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
public class SalaryTotalReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
//    throws IOException, InterruptedException{
    @Override
    protected void reduce(IntWritable k3, Iterable<IntWritable> v3, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        /*
         * context是reduce的上下文
         * 上文
         * 下文
         */
        //对v3求和
        int total = 0;
        for(IntWritable v:v3){
            total += v.get();
        }
        //输出k4单词：频率
        context.write(k3, new IntWritable(total));
    }
}

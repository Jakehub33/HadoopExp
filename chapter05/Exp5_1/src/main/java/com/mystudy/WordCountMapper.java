package com.mystudy;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class WordCountMapper extends Mapper <LongWritable, Text, Text,IntWritable> {
    //public class WordCountMapper extends Mapper <LongWritable, Text, IntWritable,IntWritable>{
    @Override
    protected void map(LongWritable key, Text value1, Context context)
            throws IOException, InterruptedException {
        //数据：Where there is a will, there is a way
        //String data = "Where there is a will, there is a way";
        String data = value1.toString();
        //String对象split()方法划分词
        /*实验一//data.split(data,'');同时用双引号不用单引号
        String[] words = data.split(" ");*/
        String[] words = data.split(" ");
        //for循环输出kv,v2
        //for(word in words){

        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }
        //取出部门号words[7]，将String转换为Int，Int转换为IntWritable对象，赋值为k2，取出工资words[5]，将String转换为Int，Int转换为IntWritable对象，赋值为v2，最终输出k2, v2
        //if( words.length>7  &&  !words[7].isEmpty()  &&   !words[6].isEmpty()) {
        /*if( words.length>7  &&  !words[7].isEmpty()) {
            context.write(new IntWritable(Integer.parseInt(words[7])),
                    new IntWritable(Integer.parseInt(words[5])));
        }*/
    }
}

package com.mystudy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalaryTotalMain {
    public static void main(String[] args) throws Exception{
        //1. 创建一个job和任务入口(指定主类)
        Job job =Job.getInstance(new Configuration());
        job.setJarByClass(SalaryTotalMain.class);
        //2. 指定job的mapper和输出的类型<k2 v2>
        //实验一job.setMapperClass(WorkCountMapper.class);
        job.setMapperClass(SalaryTotalMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        //3. 指定job的reducer和输出的类型<k4  v4>
        //实验一job.setReducerClass(WorkCountReducer.class);
        job.setReducerClass(SalaryTotalReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
//        CreateHDFSFile.createFile("/input/wordCountInput1.txt");
//        CreateHDFSFile.createFile("/output/wordCountOutput1.txt");
        //4. 指定job的输入和输出路径
        //FileInputFormat.setInputPaths(job,new Path(args[0]));
        //FileInputFormat.setInputPaths(job,new Path("hdfs://node1:8020/input/wordCountInput.txt"));
//        FileInputFormat.setInputPaths(job,new Path("hdfs://192.168.30.131:8020/input/wordCountInput.txt"));
        FileInputFormat.setInputPaths(job,new Path(args[0]));
//        FileInputFormat.setInputPaths(job,new Path(args[1]));
//        FileInputFormat.setInputPaths(job,new Path(args[2]));
        //FileOutputFormat.setOutputPaths(job.new Path(arg[1]));
        //FileOutputFormat.setOutputPath(job,new Path(args[1]));
//        FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.30.131:8020/output/wordCountOutput.txt"));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //5. 执行job
        job.waitForCompletion(true);
    }
}

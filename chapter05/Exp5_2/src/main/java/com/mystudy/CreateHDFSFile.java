package com.mystudy;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.net.URI;


public class CreateHDFSFile {
    //public static void main(String[] args) throws Exception {
    public static void createFile(String path) throws Exception {
        Configuration conf = new Configuration();
        // 配置 NameNode 地址，具体根据你的NameNode IP 配置
        URI uri = new URI("hdfs://192.168.30.131:8020");
        // 指定用户名 , 获取 FileSystem 对象
        FileSystem fs = FileSystem.get(uri, conf, "hadoop217");
        //定义文件路径
        //Path dfs = new Path("/17034460217/test2.txt");
        Path dfs = new Path(path);
        FSDataOutputStream os = fs.create(dfs, true);
        //往文件写入信息
        os.writeBytes("where there is a will there is a way");
        // 关闭流
        os.close();
        // 不需要再操作 FileSystem 了，关闭
        fs.close();
    }

}

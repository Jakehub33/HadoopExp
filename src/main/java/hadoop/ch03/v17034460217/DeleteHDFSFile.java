package hadoop.ch03.v17034460217;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.net.URI;

public class DeleteHDFSFile {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://192.168.30.131:8020");
        FileSystem fs = FileSystem.get(uri,conf,"hadoop");
        Path dfs = new Path("/17034460217/test2.txt");
        fs.delete(dfs,false);
        fs.close();
    }
}

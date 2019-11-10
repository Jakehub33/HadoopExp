package hadoop.ch03.v17034460217;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.net.URI;
import java.util.Arrays;

public class ReadHDFSFileAttr {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://192.168.30.131:8020");
        FileSystem fs = FileSystem.get(uri, conf, "hadoop");
        Path dfs = new Path("/17034460217/test5.txt");
        byte [] number = {1,7,0,3,4,4,6,0,2,1,7};
        fs.setXAttr(dfs,"user.id",number);
        System.out.println(Arrays.toString(
                fs.getXAttr(dfs,"user.id")));
    }

}
package hadoop.ch03.v17034460217;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class UploadHDFSFile {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://192.168.30.131:8020");
        FileSystem fs = FileSystem.get(uri, conf, "hadoop");
        Path dst = new Path("/17034460217");
        Path localSrc = new Path("F:/test4.txt");
        fs.copyFromLocalFile(true, true, localSrc, dst);
        fs.close();
    }

}
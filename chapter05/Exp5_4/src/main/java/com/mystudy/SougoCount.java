import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.TreeMap;

public class SougoCount {

    public static class SCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        Text text = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String keys = line[2];
            text.set(keys);
            context.write(text, new LongWritable(1));
        }
    }

    public static class SCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        //这里做了一点改动，将默认的升序改为了降序
        private static final TreeMap<Integer, String> map = new TreeMap<>((o1, o2) -> -(o1.compareTo(o2)));

        @Override
        protected void reduce(Text key, Iterable<LongWritable> value, Context context) {
            int sum = 0;
            for (LongWritable ltext : value) {
                sum += ltext.get();
            }
            map.put(sum, key.toString());
            //取前50条
            if (map.size() > 50) {
                map.remove(map.lastKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Integer count : map.keySet()) {
                context.write(new Text(map.get(count)), new LongWritable(count));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://10.211.55.10:9000");

        Job job = Job.getInstance(conf, "count");
        job.setJarByClass(SougoCount.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(SCMapper.class);
        job.setReducerClass(SCReducer.class);
        //文件路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //结果路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }


}

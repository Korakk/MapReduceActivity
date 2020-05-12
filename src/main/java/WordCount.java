import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.RegexMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\hadoop" );
        Configuration conf = new Configuration();
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        fs.delete(outputPath, true);
        String[] hashtagArgs = HashtagUtilities.findHashtags(conf, args,"(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)");
        Job job = Job.getInstance(conf, "Hash-tags counter");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(RegexMapper.class);
        job.setCombinerClass(LongSumReducer.class);
        job.setReducerClass(LongSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(hashtagArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(hashtagArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
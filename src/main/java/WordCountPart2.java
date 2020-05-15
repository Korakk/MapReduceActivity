import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.Chain;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.RegexMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class WordCountPart2 {
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        Configuration conf = new Configuration();
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        fs.delete(outputPath, true);
        Job job = Job.getInstance(conf, "Remove-Filter-Tweets");
        job.setJarByClass(WordCountPart2.class);
        ChainMapper.addMapper(job, ChainMapperUtilities.LowerCaseMapper.class, Object.class, Text.class,
                Text.class, LongWritable.class, new Configuration(false));

        ChainMapperUtilities.chainData(job, ChainMapperUtilities.JsonSelectionMapper.class);
        ChainMapperUtilities.chainData(job, ChainMapperUtilities.FilterFieldsMap.class);
        ChainMapperUtilities.chainData(job, ChainMapperUtilities.FilterLang.class);
        ChainMapperUtilities.groupData(job, LongSumReducer.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

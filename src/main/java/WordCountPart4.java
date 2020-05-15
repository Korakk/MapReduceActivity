import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class WordCountPart4 {
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Path tmpOutputPath = new Path("./tmp");
        Configuration conf1 = new Configuration();
        //conf1.set("numElems", args[2]);
        FileSystem fs = FileSystem.get(new URI(tmpOutputPath.getName()), conf1);
        fs.delete(tmpOutputPath, true);

        Job job1 = Job.getInstance(conf1, "FirstJob");
        job1.setJarByClass(WordCountPart4.class);

        ChainMapper.addMapper(job1, ChainMapperUtilities.LowerCaseMapper.class, Object.class, Text.class,
                Text.class, LongWritable.class, new Configuration(false));

        ChainMapperUtilities.chainData(job1, ChainMapperUtilities.JsonSelectionMapper.class);
        ChainMapperUtilities.chainData(job1, ChainMapperUtilities.FilterFieldsMap.class);
        ChainMapperUtilities.chainData(job1, ChainMapperUtilities.FilterLang.class);
        ChainMapperUtilities.chainData(job1, HashtagUtilities.CountHashtags.class);
        ChainMapperUtilities.groupData(job1, LongSumReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, tmpOutputPath);
        job1.waitForCompletion(true);

        //Job 2
        Configuration conf2 = new Configuration();
        conf2.set("numElems", args[2]);
        FileSystem fs2 = FileSystem.get(new URI(outputPath.toString()), conf2);
        fs2.delete(outputPath, true);
        Job job2 = Job.getInstance(conf2, "Activity2-MapReduceHadoop-Part2");
        job2.setJarByClass(WordCountPart4.class);

        job2.setMapperClass(TopNUtilities.TopNMapper.class);
        job2.setReducerClass(TopNUtilities.TopNReducer.class);

        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job2, tmpOutputPath);
        FileOutputFormat.setOutputPath(job2, outputPath);

        //Job2 is used instead of jobControl because we can't do waitForCompletion or something similar using jobControl.
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

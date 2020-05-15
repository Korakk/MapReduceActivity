import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.naming.Context;
import java.io.IOException;
import java.net.URI;
import java.util.TreeMap;

public class WordCountPart3 {

    public static class TopNMapper extends Mapper<Text, LongWritable,
                NullWritable, Text> {
        private TreeMap<LongWritable, Text> topN = new TreeMap<>();
        public void map(Text key, LongWritable value, Context context) {
            topN.put(value, new Text(key));
            if (topN.size() > Integer.parseInt(context.getConfiguration().get("numElems"))) {
                topN.remove(topN.firstKey());
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            for (Text t : topN.values()) {
                context.write(NullWritable.get(), t);
            }
        }
    }

    public static class TopNReducer extends
            Reducer<NullWritable, Text, NullWritable, Text> {

        private TreeMap<Integer, Text> topN = new TreeMap<Integer, Text>();

        @Override
        public void reduce(NullWritable key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                String[] words = value.toString().split("\t") ;

                topN.put(Integer.parseInt(words[1]),
                        new Text(value));

                if (topN.size() > 10) {
                    topN.remove(topN.firstKey());
                }
            }

            for (Text word : topN.descendingMap().values()) {
                context.write(NullWritable.get(), word);
            }
        }
    }
}

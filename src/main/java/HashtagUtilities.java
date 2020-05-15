import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.swing.text.TabExpander;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HashtagUtilities {

    public static class CountHashtags extends Mapper<Text, LongWritable, Text, LongWritable> {
        public void map(Text newKey, LongWritable value, Context context) throws IOException, InterruptedException {
            String data = newKey.toString();
            Text elem = new Text();
            JSONObject jsonData = new JSONObject(data);
            JSONArray hashTags = (JSONArray) jsonData.get("hashtags");
            for (Object hashTag : hashTags) {
                JSONArray hashtags = (JSONArray) hashTag;
                elem.set(new Text(hashtags.getString(0)));
                context.write(elem, new LongWritable(1L));
            }
        }
    }
}

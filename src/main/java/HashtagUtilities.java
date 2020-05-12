import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.swing.text.TabExpander;
import java.io.IOException;

public class HashtagUtilities {

    public static class CountHashtags extends Mapper<Object, Text, Text, LongWritable> {
        public void map(Object newKey, Text value, Context context) throws IOException, InterruptedException {
            String data = value.toString();
            Text elem = new Text();
            JSONObject jsonData = new JSONObject(data);
            JSONObject entities = (JSONObject) jsonData.get("entities");
            JSONArray hashTags = (JSONArray) entities.get("hashtags");
            for (Object hashTag : hashTags) {
                JSONObject jsonHashtag = (JSONObject) hashTag;

                elem.set(new Text(jsonHashtag.get("text").toString()));
                context.write(elem, new LongWritable(1L));
            }
        }
    }
}

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;

public class ChainMapperUtilities {

    public static class LowerCaseMapper extends Mapper<Object, Text, Text, LongWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String val = value.toString().toLowerCase();
            Text newKey = new Text(val);
            //Invertimos el value en el sitio del key porque asi se trabajara mas facil en un futuro.
            context.write(newKey, new LongWritable(1));
        }
    }

    public static class JsonSelectionMapper extends Mapper<Text, LongWritable, Text, LongWritable> {
        public void map(Text newKey, LongWritable value, Context context) throws IOException, InterruptedException {
            String data = newKey.toString();
            JSONObject jsonData = new JSONObject(data);
            JSONObject entities = (JSONObject) jsonData.get("entities");
            JSONArray hashTags = (JSONArray) entities.get("hashtags");
            String text = (String) jsonData.get("text");
            if(!hashTags.isEmpty() && !text.equals("")){
                context.write(newKey, value);
            }
        }
    }

    public static class FilterFieldsMap extends Mapper<Text, LongWritable, Text, LongWritable> {
        public void map(Text newKey, LongWritable value, Context context) throws IOException, InterruptedException {
            String data = newKey.toString();
            JSONObject jsonObject = new JSONObject(data);
            JSONObject entities = (JSONObject) jsonObject.get("entities");
            JSONObject filteredData = new JSONObject();
            filteredData.append("hashtags", entities.get("hashtags"));
            filteredData.append("text", jsonObject.get("text"));
            filteredData.append("lang", jsonObject.get("lang"));
            Text filteredNewKey = new Text(filteredData.toString());
            context.write(filteredNewKey, value);
        }
    }

    public static class FilterLang extends Mapper<Text, LongWritable, Text, LongWritable> {
        public void map(Text newKey, LongWritable value, Context context) throws IOException, InterruptedException {
            String data = newKey.toString();
            JSONObject jsonObject = new JSONObject(data);
            JSONArray langs = (JSONArray) jsonObject.get("lang");
            if(langs.toString().contains("\"es\"")){
                context.write(newKey, value);
            }
        }
    }


    public static void chainData(Job job, Class<? extends Mapper> chainMap) throws IOException {
        ChainMapper.addMapper(job, chainMap, Text.class, LongWritable.class,
                Text.class, LongWritable.class, new Configuration(false));
    }

    public static void groupData(Job job, Class<? extends Reducer> reduceClass) throws IOException {
        ChainReducer.setReducer(job, reduceClass, Text.class, LongWritable.class,
                Text.class, LongWritable.class, new Configuration(false));
    }

}

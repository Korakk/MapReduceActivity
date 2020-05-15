import javafx.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

public class TopNUtilities {

    public static class TopNMapper extends Mapper<Text, Text,
            NullWritable, Text> {
        List<Pair<Integer, Text>> toBeSorted = new ArrayList<>();
        List<Pair<Integer, Text>> sortedList = new ArrayList<>();
        public void map(Text key, Text value, Context context) {
            toBeSorted.add(new Pair<>(Integer.parseInt(value.toString()), new Text(key+"\t"+value)));
            sortedList = CustomPairListSort.sortedListCreation(toBeSorted);
            if (sortedList.size() > 2*Integer.parseInt(context.getConfiguration().get("numElems"))) {
                Collections.reverse(sortedList);
                sortedList.remove(0);
            }
        }

        /**
         * No podemos ejecutar el reduce, no tenemos key!
         * Por ese motivo se crea el cleanup y se añade un NullWritable.get() como key porque queremos que todos los
         * valores estén asociados con el mismo clave para que podamos realizar un pedido global en todos los
         * mappers’ top n lists.
         * topN.values() --> words
         * key --> NullWritable.get()
         *
         * **/
        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            for (Text t : CustomPairListSort.getAllValues(toBeSorted)) {
                context.write(NullWritable.get(), t);
            }
        }
    }

    public static class TopNReducer extends
            Reducer<NullWritable, Text, NullWritable, Text> {

        @Override
        public void reduce(NullWritable key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            List<Pair<Integer, Text>> toBeSorted = new ArrayList<>();
            List<Pair<Integer, Text>> sortedList = new ArrayList<>();

            for (Text value : values) {
                String[] words = value.toString().split("\t");

                toBeSorted.add(new Pair<>(Integer.parseInt(words[1]),
                        new Text(value)));
                sortedList = CustomPairListSort.sortedListCreation(toBeSorted);

                if (sortedList.size() > Integer.parseInt(context.getConfiguration().get("numElems"))) {
                    Collections.reverse(sortedList);
                    sortedList.remove(0);
                }
            }
            Collections.reverse(sortedList);
            for (Text word : CustomPairListSort.getAllValues(sortedList)) {
                context.write(NullWritable.get(), word);
            }
        }
    }
}

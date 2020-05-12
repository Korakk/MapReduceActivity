import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class HashtagUtilities {

    public static String[] findHashtags(Configuration conf, String[] args, String rgx) throws IOException {
        conf.set("mapreduce.mapper.regex", rgx);
        return new GenericOptionsParser(conf, args).getRemainingArgs();
    }
}

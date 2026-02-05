import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class PopularPagesReducer extends Reducer<NullWritable, Text, Text, NullWritable> {

    private TreeMap<Integer, String> popularPages = new TreeMap<>();

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text val : values) {
            String[] fields = val.toString().split(",");
            int count = Integer.parseInt(fields[0]);
            popularPages.put(count, val.toString());
            if (popularPages.size() > 10) {
                popularPages.remove(popularPages.firstKey());
            }
        }

        for (String val : popularPages.descendingMap().values()) {
            context.write(new Text(val), NullWritable.get());
        }
    }
}

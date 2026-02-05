import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class PopularPagesMapper extends Mapper<Object, Text, NullWritable, Text> {

    private Map<Integer, String> pageInfo = new HashMap<>();
    private TreeMap<Integer, String> popularPages = new TreeMap<>();

    @Override
    protected void setup(Context context) throws IOException {
        Path[] cacheFiles = context.getLocalCacheFiles();
        BufferedReader reader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] fields = line.split(",");
            int id = Integer.parseInt(fields[0]);
            String info = fields[1] + "," + fields[2]; // NickName, JobTitle
            pageInfo.put(id, info);
        }
        reader.close();
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        int pageId = Integer.parseInt(fields[0]);
        int count = Integer.parseInt(fields[1]);

        String info = pageInfo.get(pageId);
        if (info != null) {
            popularPages.put(count, pageId + "," + info);
            if (popularPages.size() > 10) {
                popularPages.remove(popularPages.firstKey()); // remove smallest count
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String val : popularPages.descendingMap().values()) {
            context.write(NullWritable.get(), new Text(val));
        }
    }
}

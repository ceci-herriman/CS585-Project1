import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;


/*Find the 10 most popular CircleNetPages, namely, those that got the most accesses based
on the ActivityLog among all pages. Return Id, NickName, and JobTitle.*/

public class taskB {

    // Mapper
    public static class AccessCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text pageId = new Text();
        private static final IntWritable outputValue = new IntWritable(1);
        
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            // 1. get a line at a time from the input data file
            String line = value.toString();

            // 2. divide the line we got from step 1 into words by specifying the text delimiter " "
            String[] fields = line.split(" ");

            // ActivityLog: userId,pageId,timestamp
            if (fields.length >= 2) {
                pageId.set(fields[1]);
                context.write(pageId, outputValue);
            }
        }
    }

    // Reducer
    public static class Top10Reducer
            extends Reducer<Text, IntWritable, Text, Text> {

        private TreeMap<Integer, String> topPages = new TreeMap<>();
        private Map<String, String> pageInfo = new HashMap<>();

        // Load CircleNetPage.txt
        protected void setup(Context context) throws IOException {
            BufferedReader br = new BufferedReader(
                    new FileReader("/home/ds503/CircleNetPage.txt"));

            String line;
            while ((line = br.readLine()) != null) {
                String[] vals = line.split(",");
                if (vals.length >= 3) {
                    String id = vals[0];
                    String nickJob = vals[1] + ", " + vals[2];
                    pageInfo.put(id, nickJob);
                }
            }
            br.close();
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            topPages.put(sum, key.toString());

            if (topPages.size() > 10) {
                topPages.remove(topPages.firstKey());
            }
        }

        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            for (Map.Entry<Integer, String> entry : topPages.descendingMap().entrySet()) {
                String pageId = entry.getValue();
                int count = entry.getKey();

                String info = pageInfo.getOrDefault(pageId, "Unknown, Unknown");
                context.write(
                        new Text(pageId),
                        new Text(info + "\t" + count)
                );
            }
        }
    }


    // Driver
    public static void main(String[] args)
            throws Exception {

        // 1. create a job object
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top 10 Popular Pages");

        // 2. map the class
        job.setJarByClass(taskB.class);

        // 3. both the mapper class and reducer class
        job.setMapperClass(AccessCountMapper.class);
        job.setReducerClass(Top10Reducer.class);
        job.setNumReduceTasks(1);

        // 4. set up the output key value data type class
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5. set up the final output key value data type class (It doesn't have to be the result of a reducer.)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 6. Specify the input and output path
        FileInputFormat.setInputPaths(
                job,
                new Path("file:///home/ds503/ActivityLog.txt")
        );

        FileOutputFormat.setOutputPath(
                job,
                new Path("file:///home/ds503/shared_folder/project1/taskB/output")
        );

        // 7. submit the job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}

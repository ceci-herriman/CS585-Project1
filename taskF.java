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

/*
Report all owners of a CircleNetPage who have more followers
than the average number of followers.
*/

public class taskF {

    // Mapper
    public static class FollowerCountMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text pageOwner = new Text();
        private static final IntWritable outputValue = new IntWritable(1);
        

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

            // ColRel,ID1,ID2,Date,Desc
            // 1. get a line at a time from the input data file
            String line = value.toString();

            // 2. divide the line we got from step 1 into words by specifying the text delimiter " "
            String[] fields = line.split(" ");

            if (fields.length >= 3) {
                pageOwner.set(fields[2]); // ID2 = page owner
                context.write(pageOwner, outputValue);
            }
        }
    }

    // Reducer
    public static class PopularityReducer
            extends Reducer<Text, IntWritable, Text, Text> {

        private Map<String, String> pageInfo = new HashMap<>();
        private Map<String, Integer> followerCounts = new HashMap<>();

        private long totalFollowers = 0;
        private long totalUsers = 0;

        // Load CircleNetPage.txt
        protected void setup(Context context) throws IOException {
            BufferedReader br = new BufferedReader(
                    new FileReader("/home/ds503/CircleNetPage.txt"));

            String line;
            while ((line = br.readLine()) != null) {
                String[] vals = line.split(",");
                if (vals.length >= 3) {
                    pageInfo.put(vals[0], vals[1] + ", " + vals[2]); // NickName, JobTitle
                }
            }
            br.close();
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int count = 0;
            for (IntWritable v : values) {
                count += v.get();
            }

            followerCounts.put(key.toString(), count);
            totalFollowers += count;
            totalUsers++;
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            double averageFollowers =
                    (double) totalFollowers / totalUsers;

            for (Map.Entry<String, Integer> entry : followerCounts.entrySet()) {
                if (entry.getValue() > averageFollowers) {
                    String info = pageInfo.getOrDefault(
                            entry.getKey(), "Unknown, Unknown");

                    context.write(
                            new Text(entry.getKey()),
                            new Text(info + "\t" + entry.getValue())
                    );
                }
            }
        }
    }

    // Driver
    public static void main(String[] args) 
            throws Exception {

        // 1. create a job object
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Users More Popular Than Average");

        // 2. map the class
        job.setJarByClass(taskF.class);

        // 3. both the mapper class and reducer class
        job.setMapperClass(FollowerCountMapper.class);
        job.setReducerClass(PopularityReducer.class);
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
                new Path("file:///home/ds503/Follows.txt")
        );

        FileOutputFormat.setOutputPath(
                job,
                new Path("file:///home/ds503/shared_folder/project1/taskF/output")
        );

        // 7. submit the job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
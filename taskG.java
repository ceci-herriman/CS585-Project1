import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
scp -P 14226 C:/Users/op902/CS585-Project1/taskG.java ds503@localhost:~/

compile and run instrutions I used:
javac -classpath $(hadoop classpath) taskG.java
jar cf taskg.jar taskG*.class
rm -rf ~/shared_folder/project1/taskG/output
hadoop jar taskg.jar taskG

cat ~/shared_folder/project1/taskG/output/part-r-00000
*/
public class taskG {
    // Mapper
    public static class ActivityMapper
            extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        private IntWritable userId = new IntWritable();
        private IntWritable time = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");
            if (fields.length >= 5) {
                try {
                    int byWho = Integer.parseInt(fields[1].trim());
                    int activityTime = Integer.parseInt(fields[4].trim());

                    if (activityTime >= 24 * 90) { // 24 hours * 90 days
                        userId.set(byWho);
                        time.set(activityTime);
                        context.write(userId, time);
                    }
                } catch (NumberFormatException e) {
                    // skip bad tuples
                }
            }
        }
    }

    // Reducer
    public static class RecentUserReducer
            extends Reducer<IntWritable, IntWritable, Text, NullWritable> {

        private Map<Integer, String> userInfo = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            // Load CircleNetPage.txt
            BufferedReader br = new BufferedReader(
                    new FileReader("CircleNetPage.txt")
            );
            String line;
            while ((line = br.readLine()) != null) {
                String[] f = line.split(",");
                if (f.length >= 2) {
                    userInfo.put(
                            Integer.parseInt(f[0].trim()),
                            f[1].trim()
                    );
                }
            }
            br.close();
        }

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values,
                              Context context)
                throws IOException, InterruptedException {

            // If reducer is called, user has activity >= threshold
            String nickname = userInfo.get(key.get());
            if (nickname != null) {
                context.write(
                        new Text(key.get() + "," + nickname),
                        NullWritable.get()
                );
            }
        }
    }

    // Driver
    public static void main(String[] args)
            throws Exception {

        // 1. create a job object
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Get Recent CircleNetPages");

        // 2. map the class
        job.setJarByClass(taskG.class);

        // 3. both the mapper class and the reducer class
        job.setMapperClass(ActivityMapper.class);
        job.setReducerClass(RecentUserReducer.class);
        job.setNumReduceTasks(1);

        // 4. set up the output key value data type class
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5. set up the final output key value data type class (It doesn't have to be the result of a reducer.)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 6. Specify the input and output path
        FileInputFormat.setInputPaths(
                job,
                new Path("file:///home/ds503/ActivityLog.txt")
        );

        FileOutputFormat.setOutputPath(
                job,
                new Path("file:///home/ds503/shared_folder/project1/taskG/output")
        );

        // 7. submit the job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}

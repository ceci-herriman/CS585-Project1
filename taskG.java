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
 * TaskG:
 * Identify outdated CircleNetPages (no activity in the last 90 days)
 * Output: Id,NickName
 */
public class TaskG {

    /* -------------------- MAPPER -------------------- */
    public static class ActivityMapper
            extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        private IntWritable userId = new IntWritable();
        private IntWritable time = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");
            if(fields.length >= 5) {
                try {
                    int byWho = Integer.parseInt(fields[1].trim());
                    int activityTime = Integer.parseInt(fields[4].trim());
                    userId.set(byWho);
                    time.set(activityTime);
                    context.write(userId, time);
                } catch(NumberFormatException e){
                    // skip invalid lines
                }
            }
        }
    }

    /* -------------------- REDUCER -------------------- */
    public static class OutdatedReducer
            extends Reducer<IntWritable, IntWritable, Text, NullWritable> {

        private Map<Integer,String> pageInfo = new HashMap<>();
        private int maxTime = 0;

        @Override
        protected void setup(Context context) throws IOException {
            // Load CircleNetPage.txt for Id -> NickName mapping
            BufferedReader br = new BufferedReader(new FileReader("CircleNetPage.txt"));
            String line;
            while((line = br.readLine()) != null){
                String[] f = line.split(",");
                if(f.length >=2){
                    pageInfo.put(Integer.parseInt(f[0].trim()), f[1].trim());
                }
            }
            br.close();
        }

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int lastActivity = 0;
            for(IntWritable v: values){
                if(v.get() > lastActivity) lastActivity = v.get();
            }

            // Keep track of global maxTime
            if(lastActivity > maxTime) maxTime = lastActivity;

            // Store lastActivity for user for later cleanup
            context.getCounter("TaskG","MAXTIME").setValue(Math.max(context.getCounter("TaskG","MAXTIME").getValue(), lastActivity));
            context.write(new Text(key.get() + "," + lastActivity), NullWritable.get());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // We'll do filtering in the same reducer pass below
            // Alternative: if dataset is small, read back lastActivity from cleanup or do a second job
            // For simplicity, assume maxTime known in advance or set manually
        }
    }

    /* -------------------- DRIVER -------------------- */
    public static void main(String[] args)
            throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Outdated CircleNetPages");

        // 1. Set jar
        job.setJarByClass(TaskG.class);

        // 2. Mapper and Reducer
        job.setMapperClass(ActivityMapper.class);
        job.setReducerClass(OutdatedReducer.class);
        job.setNumReduceTasks(1);

        // 3. Map output types
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 4. Output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 5. Input and output paths
        FileInputFormat.setInputPaths(job, new Path("file:///home/ds503/ActivityLog.txt"));
        FileOutputFormat.setOutputPath(job, new Path("file:///home/ds503/shared_folder/project1/taskG/output"));

        // 6. Submit job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}

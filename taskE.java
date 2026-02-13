import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class taskE {
    // Custom Writable class to hold an array of IntWritable values (page and count)
    public static class IntArrayWritable extends ArrayWritable {
    public IntArrayWritable() {
        super(IntWritable.class);
    }

    public IntArrayWritable(IntWritable[] values) {
        super(IntWritable.class, values);
    }

    @Override
    public Writable[] get() {
        return super.get();
    }

    @Override
    public String toString() {
        Writable[] values = get();
        return values[0].toString() + ", " + values[1].toString();
    }
}


    // Mapper class to map who accessed what page
    public static class TaskEMapper extends Mapper<LongWritable, Text, Text, IntArrayWritable> {
        private Text user = new Text();
        private IntWritable page = new IntWritable();
        //private IntWritable[] outValue = new IntWritable[2]; // to hold page and count of 1 for each access
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // ActivityLog: ActionId,ByWho,WhatPage,ActionType,ActionTime
            String[] fields = value.toString().split(",", -1);
            if (fields.length < 5) return;

            String userId = fields[1].trim();
            int pageId = Integer.parseInt(fields[2].trim());

            user.set(userId);

            IntWritable[] arr = new IntWritable[] { new IntWritable(pageId), new IntWritable(1) };
            context.write(user, new IntArrayWritable(arr));
        }
        }

    // Combiner class to sum up the counts for each page accessed by a user and count unique pages accessed
        public static class TaskEReducer extends Reducer<Text, IntArrayWritable, Text, IntArrayWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntArrayWritable> values, Context context)
            throws IOException, InterruptedException {

            int total = 0;
            Set<Integer> distinct = new HashSet<>();

            for (IntArrayWritable v : values) {
            Writable[] w = v.get(); // IntArrayWritable returns IntWritable[]
            int page = ((IntWritable) w[0]).get();
            int cnt = ((IntWritable) w[1]).get();
            total += cnt;
            distinct.add(page);
            }

            IntWritable[] out = new IntWritable[] { new IntWritable(total), new IntWritable(distinct.size()) };
            context.write(key, new IntArrayWritable(out));
        }
        }

    // Driver
    public static void main(String[] args) throws Exception {
        
        // 1. create a job object
        Configuration conf = new Configuration();
        Job job =
            Job.getInstance(conf, "User Page Access Count and Unique Pages Accessed");

        // 2. map the class
        job.setJarByClass(taskE.class);

        //job.setInputFormatClass(TextInputFormat.class);

        // 3. both the mapper class and the reducer class
        job.setMapperClass(TaskEMapper.class);
        job.setReducerClass(TaskEReducer.class);
        job.setNumReduceTasks(1); // single reducer to compute global average

        // 4. set up the output key value data type class
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntArrayWritable.class);

        // 5. set up the final output key value data type class (It doesn't have to be the result of a reducer.)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntArrayWritable.class);

        // 6. Specify the input and output path
        FileInputFormat.setInputPaths(
            job, new Path("file:///home/ds503/ActivityLog.txt"));
        FileOutputFormat.setOutputPath(job,
            new Path("file:///home/ds503/shared_folder/project1/taskE/output"));

        // 7. submit the job

        long startTime = System.nanoTime();
        boolean result = job.waitForCompletion(true);
        long endTime = System.nanoTime();

        double durationMilli = (double) (endTime - startTime) / 1000000.0;
        System.out.println("Time to complete in milliseconds: " + durationMilli);

        System.exit(result ? 0 : 1);

}

}
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


public class taskA {
    // Mapper class to count the occurrences of each hobby
    public static class taskAMapper 
    extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text hobby = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 5) {
                try {
                    String hobbyStr = fields[4].trim();
                    hobby.set(hobbyStr);
                    context.write(hobby, one);
                } catch (NumberFormatException e) {
                    // skip bad lines
                }
            }
        }
    }

    // Reducer class to sum up the counts for each hobby, pulled from WordCount example
    public static class HobbyCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    private IntWritable outV = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        // get the total count of word occurrences for each word (input key)
        for (IntWritable value : values) {
            sum += value.get(); // The data type of value is already IntWritable, to get its value, we need to call a getter
        }
        outV.set(sum);

        // write out the result (output (key, value) pair of reduce phase)
        context.write(key,outV);
    }
}



    //Driver
        public static void main(String[] args) throws Exception {
        
        // 1. create a job object
        Configuration conf = new Configuration();
        Job job =
            Job.getInstance(conf, "Frequency of Hobbies");

        // 2. map the class
        job.setJarByClass(taskA.class);

        // 3. both the mapper class and the reducer class
        job.setMapperClass(taskAMapper.class);
        job.setReducerClass(HobbyCountReducer.class);
        job.setNumReduceTasks(1); // single reducer to compute global average

        // 4. set up the output key value data type class
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5. set up the final output key value data type class (It doesn't have to be the result of a reducer.)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6. Specify the input and output path
        FileInputFormat.setInputPaths(
            job, new Path("file:///home/ds503/CircleNetPage.txt"));
        FileOutputFormat.setOutputPath(job,
            new Path("file:///home/ds503/shared_folder/CS585-Project1/taskA/output"));

        // 7. submit the job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }

}

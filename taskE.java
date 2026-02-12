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

public class taskE {
    // Custom Writable class to hold an array of IntWritable values (page and count)
    public static class IntArrayWritable extends ArrayWritable {

    public IntArrayWritable(IntWritable[] values) {
        super(IntWritable.class, values);
    }

    @Override
    public IntWritable[] get() {
        return (IntWritable[]) super.get();
    }

    @Override
    public String toString() {
        IntWritable[] values = get();
        return values[0].toString() + ", " + values[1].toString();
    }
}


    // Mapper class to map who accessed what page
    public static class TaskEMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text page = new Text();
        private Text user = new Text();
        //private IntWritable[] outValue = new IntWritable[2]; // to hold page and count of 1 for each access

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 3) {
                try {
                    String userId = fields[0].trim();
                    String pageId = fields[1].trim();
                    user.set(userId);
                    page.set(pageId);
                    //IntWritable[] temp = new IntWritable[2];
                    //IntArrayWritable outValue = new IntArrayWritable(temp);
                    //temp[0].set(pageId.get());
                    //temp[1].set(1); // set count of 1 for each access
                    //outValue.set(temp);
                    context.write(user, page); // Emit (user, [page, 1]) for each page access
                } catch (NumberFormatException e) {
                    // skip bad lines
                }
            }
        }
    }

    // Combiner class to sum up the counts for each page accessed by a user and count unique pages accessed
    public static class TaskECombiner
     extends Reducer<Text, Text, Text, IntArrayWritable> {
        private IntWritable outCount = new IntWritable();
        private IntWritable outUnique = new IntWritable();


        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, IntArrayWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0; //for total # of accesses
            Set<Text> uniquePages = new HashSet<>(); //to count unique pages accessed by the user
            // get the total count of page accesses for each page and count unique pages accessed by the user
            for (Text val : values) {
                sum++; // increment total accesses for each page access
                uniquePages.add(val);
            }
            IntWritable accesses = new IntWritable(sum);
            IntWritable[] temp = new IntWritable[2];
            IntArrayWritable output = new IntArrayWritable(temp); // create an array to hold total accesses and unique pages accessed

            temp[0].set(accesses.get());
            temp[1].set(uniquePages.size());
            
            output.set(temp);
            context.write(key, new IntArrayWritable(output.get())); // Emit (user, total accesses, unique pages accessed)
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

        // 3. both the mapper class and the reducer class
        job.setMapperClass(TaskEMapper.class);
        job.setCombinerClass(TaskECombiner.class);
        job.setNumReduceTasks(1); // single reducer to compute global average

        // 4. set up the output key value data type class
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 5. set up the final output key value data type class (It doesn't have to be the result of a reducer.)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntArrayWritable.class);

        // 6. Specify the input and output path
        FileInputFormat.setInputPaths(
            job, new Path("file:///home/ds503/Follows.txt"));
        FileOutputFormat.setOutputPath(job,
            new Path("file:///home/ds503/shared_folder/project1/taskE/output"));

        // 7. submit the job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

}

}
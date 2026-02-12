import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;


public class taskD {
    // Mapper class to map who follows whom
    public static class TaskDMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text user = new Text();
        private IntWritable one = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 2) {
                try {
                    String followerId = fields[0].trim();
                    String followeeId = fields[1].trim();
                    user.set(followeeId);
                    context.write(user, one); // Emit (followee, 1) for each follower
                } catch (NumberFormatException e) {
                    // skip bad lines
                }
            }
        }
    }

    // Reducer class to sum up the counts of followers for each user
    public static class TaskDReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Map<Integer,String> pageInfo = new HashMap<>();
        private IntWritable outCount = new IntWritable(0);

        @Override
        protected void setup(Context context) throws IOException {
            // Load CircleNetPage.txt for Id -> Nickname mapping
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
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String username = pageInfo.get(Integer.parseInt(key.toString()));
            int sum = 0; //for total # of followers
            for (IntWritable val : values) {
                sum += val.get();

            }
            outCount.set(sum);
            context.write(new Text(username), outCount); // Emit (user, total followers)
            pageInfo.remove(Integer.parseInt(key.toString())); // remove the user from pageInfo to keep track of users with zero followers
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Emit users with zero followers
            for(Map.Entry<Integer,String> entry : pageInfo.entrySet()){
                context.write(new Text(entry.getValue()), new IntWritable(0)); // Emit (user, 0) for users with no followers
            }
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task D");
        job.setJarByClass(taskD.class);

        // 1. set up the mapper
        job.setMapperClass(TaskDMapper.class);

        // 2. set up the reducer
        job.setReducerClass(TaskDReducer.class);

        // 3. set up the output key value data type class
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 4. set up the final output key value data type class (It doesn't have to be the result of a reducer.)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 5. Specify the input and output path
        FileInputFormat.setInputPaths(
            job, new Path("file:///home/ds503/Follows.txt"));
        FileOutputFormat.setOutputPath(job,
            new Path("file:///home/ds503/shared_folder/project1/taskD/output"));

        // 6. submit the job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

}
}
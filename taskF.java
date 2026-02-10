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
 * TaskF:
 * Report all owners of a CircleNetPage who have more followers than the average number of followers.
 * Output: ownerId,NickName,JobTitle    followerCount
 */

/*
scp -P 14226 C:/Users/op902/CS585-Project1/taskB.java ds503@localhost:~/

compile and run instrutions I used: 
javac -classpath $(hadoop classpath) taskF.java
jar cf taskf.jar taskF*.class
rm -rf ~/shared_folder/project1/taskF/output 
hadoop jar taskf.jar taskF

cat ~/shared_folder/project1/taskF/output/part-r-00000 
*/
public class taskF {

    /* -------------------- MAPPER -------------------- */
    public static class FollowerCountMapper
            extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        private final static IntWritable ONE = new IntWritable(1);
        private IntWritable ownerId = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");
            if (fields.length >= 3) {
                try {
                    int pageOwnerId = Integer.parseInt(fields[2].trim()); // id2 = page owner
                    ownerId.set(pageOwnerId);
                    context.write(ownerId, ONE);
                } catch(NumberFormatException e) {
                    // skip invalid lines
                }
            }
        }
    }

    /* -------------------- REDUCER -------------------- */
    public static class AboveAverageReducer
            extends Reducer<IntWritable, IntWritable, Text, IntWritable> {

        private Map<Integer,Integer> counts = new HashMap<>();
        private int totalFollowers = 0;
        private int totalOwners = 0;

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for(IntWritable v : values){
                sum += v.get();
            }
            counts.put(key.get(), sum);
            totalFollowers += sum;
            totalOwners++;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            double avg = (double) totalFollowers / totalOwners;

            // Load CircleNetPage.txt for owner info
            Map<Integer,String> ownerInfo = new HashMap<>();
            BufferedReader br = new BufferedReader(new FileReader("CircleNetPage.txt"));
            String line;
            while((line = br.readLine()) != null){
                String[] f = line.split(",");
                if(f.length >=3){
                    ownerInfo.put(Integer.parseInt(f[0].trim()), f[1].trim() + "," + f[2].trim());
                }
            }
            br.close();

            // Emit only owners above average
            for(Map.Entry<Integer,Integer> e : counts.entrySet()){
                if(e.getValue() > avg){
                    String info = ownerInfo.getOrDefault(e.getKey(), "UNKNOWN,UNKNOWN");
                    context.write(new Text(e.getKey() + "," + info), new IntWritable(e.getValue()));
                }
            }
        }
    }

    /* -------------------- DRIVER -------------------- */
    public static void main(String[] args)
            throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CircleNet Owners Above Average Followers");

        // 1. Set jar
        job.setJarByClass(taskF.class);

        // 2. Mapper and Reducer
        job.setMapperClass(FollowerCountMapper.class);
        job.setReducerClass(AboveAverageReducer.class);
        job.setNumReduceTasks(1); // single reducer to compute global average

        // 3. Map output key/value types
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 4. Final output key/value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 5. Input and output paths
        FileInputFormat.setInputPaths(job, new Path("file:///home/ds503/Follows.txt"));
        FileOutputFormat.setOutputPath(job, new Path("file:///home/ds503/shared_folder/project1/taskF/output"));

        // 6. Submit job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}

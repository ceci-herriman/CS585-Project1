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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/*Find the 10 most popular CircleNetPages, namely, those that got the most accesses based
on the ActivityLog among all pages. Return Id, NickName, and JobTitle.*/

/*
scp -P 14226 C:/Users/op902/CS585-Project1/taskB.java ds503@localhost:~/

compile and run instrutions I used: 
javac -classpath $(hadoop classpath) taskB.java
jar cf taskb.jar taskB*.class
rm -rf ~/shared_folder/project1/taskB/output 
hadoop jar taskb.jar taskB

cat ~/shared_folder/project1/taskB/output/part-r-00000 
*/
public class taskB {

    /* -------------------- MAPPER 1 --------------------
     * Reads ActivityLog.txt
     * Emits: (pageId, 1)
     */
    public static class AccessCountMapper
            extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);
        private IntWritable pageId = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = line.split(",");

            // activityId, byWho, whatPage, actionType, time
            if (fields.length >= 3) {
                try {
                    int whatPage = Integer.parseInt(fields[2].trim());
                    pageId.set(whatPage);
                    context.write(pageId, ONE);
                } catch (NumberFormatException e) {
                    // ignore bad records
                }
            }
        }
    }

    /* -------------------- REDUCER 1 --------------------
     * Sums page accesses
     * Output: (pageId, totalCount)
     */
    public static class AccessCountReducer
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values,
                              Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    /* -------------------- MAPPER 2 --------------------
     * Swaps to (count, pageId) so we can sort
     */
    public static class Top10Mapper
            extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        private IntWritable count = new IntWritable();
        private IntWritable pageId = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split("\t");
            if (parts.length == 2) {
                pageId.set(Integer.parseInt(parts[0]));
                count.set(Integer.parseInt(parts[1]));
                context.write(count, pageId);
            }
        }
    }

    /* -------------------- REDUCER 2 --------------------
     * Emits only Top 10 pages
     * Also joins CircleNetPage.txt
     */
    public static class Top10Reducer
            extends Reducer<IntWritable, IntWritable, Text, IntWritable> {

        private int counter = 0;
        private Map<Integer, String> pageInfo = new HashMap<>();

        // Load CircleNetPage.txt from Distributed Cache
        @Override
        protected void setup(Context context)
                throws IOException {

            Path[] files = context.getLocalCacheFiles();
            if (files == null) return;

            for (Path p : files) {
                BufferedReader br = new BufferedReader(new FileReader(p.toString()));
                String line;
                while ((line = br.readLine()) != null) {
                    // Id,NickName,JobTitle,Region,Hobby
                    String[] f = line.split(",");
                    if (f.length >= 3) {
                        pageInfo.put(
                                Integer.parseInt(f[0].trim()),
                                f[1].trim() + "," + f[2].trim()
                        );
                    }
                }
                br.close();
            }
        }

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values,
                              Context context)
                throws IOException, InterruptedException {

            for (IntWritable pageId : values) {
                if (counter < 10) {
                    String info = pageInfo.getOrDefault(
                            pageId.get(),
                            "UNKNOWN,UNKNOWN"
                    );
                    // Output: Id,NickName,JobTitle    Count
                    context.write(
                            new Text(pageId.get() + "," + info),
                            key
                    );
                    counter++;
                }
            }
        }
    }

    /* -------------------- SORT COMPARATOR --------------------
     * Sorts counts in descending order
     */
    public static class DescendingIntComparator
            extends WritableComparator {

        protected DescendingIntComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -1 * a.compareTo(b);
        }
    }

    // Driver
    public static void main(String[] args)
        throws Exception {

    /* =========================
       JOB 1: Count Page Accesses
       ========================= */

    // 1. create a job object
    Configuration conf1 = new Configuration();
    Job job1 = Job.getInstance(conf1, "Page Access Count");

    // 2. map the class
    job1.setJarByClass(taskB.class);

    // 3. both the mapper class and reducer class
    job1.setMapperClass(AccessCountMapper.class);
    job1.setReducerClass(AccessCountReducer.class);
    job1.setNumReduceTasks(1);

    // 4. set up the output key value data type class
    job1.setMapOutputKeyClass(IntWritable.class);
    job1.setMapOutputValueClass(IntWritable.class);

    // 5. set up the final output key value data type class
    job1.setOutputKeyClass(IntWritable.class);
    job1.setOutputValueClass(IntWritable.class);

    // 6. Specify the input and output path
    FileInputFormat.setInputPaths(
            job1,
            new Path("file:///home/ds503/ActivityLog.txt")
    );

    FileOutputFormat.setOutputPath(
            job1,
            new Path("file:///home/ds503/shared_folder/project1/taskB/temp")
    );

    // 7. submit the job
    boolean result1 = job1.waitForCompletion(true);

    if (!result1) {
        System.exit(1);
    }

    /* =========================
       JOB 2: Top 10 Pages
       ========================= */

    // 1. create a job object
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "Top 10 Popular Pages");

    // 2. map the class
    job2.setJarByClass(taskB.class);

    // 3. both the mapper class and reducer class
    job2.setMapperClass(Top10Mapper.class);
    job2.setReducerClass(Top10Reducer.class);
    job2.setNumReduceTasks(1);

    // 4. set up the output key value data type class
    job2.setMapOutputKeyClass(IntWritable.class);
    job2.setMapOutputValueClass(IntWritable.class);

    // 5. set up the final output key value data type class
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);

    // 6. Specify the input and output path
    FileInputFormat.setInputPaths(
            job2,
            new Path("file:///home/ds503/shared_folder/project1/taskB/temp")
    );

    FileOutputFormat.setOutputPath(
            job2,
            new Path("file:///home/ds503/shared_folder/project1/taskB/output")
    );

    // 7. submit the job
    boolean result2 = job2.waitForCompletion(true);

    System.exit(result2 ? 0 : 1);
}
}
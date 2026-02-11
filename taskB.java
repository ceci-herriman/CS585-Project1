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

/*Find the 10 most popular CircleNetPages, namely, those that got the most
accesses based on the ActivityLog among all pages. Return Id, NickName, and
JobTitle.*/

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
    // -------------------- JOB 1 --------------------
    // Mapper for Job 1
    public static class AccessCountMapper
        extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private IntWritable pageId = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            // each line has the format: activityId, byWho, whatPage, actionType, time
            if (fields.length >= 3) {
                try {
                    int whatPage = Integer.parseInt(fields[2].trim());
                    pageId.set(whatPage);
                    context.write(pageId, ONE);
                } catch (NumberFormatException e) {
                    // skip bad tuples
                }
            }
        }
    }

    // Reducer for Job 1
    public static class AccessCountReducer
        extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    // -------------------- JOB 2 --------------------
    // Mapper for job 2:
    public static class Top10Mapper
        extends Mapper<LongWritable, Text, IntWritable, Text> {
        private IntWritable pageId = new IntWritable();
        private Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty())
                return;

            String[] parts = line.split("[,\t]"); // Job1 output: pageId \t count

            if (parts.length == 2 && line.contains("\t")) {
                
                pageId.set(Integer.parseInt(parts[0].trim()));
                outValue.set("C," + parts[1].trim()); // C = count
                context.write(pageId, outValue);

            } else if (parts.length >= 3) {
                // CircleNetPage.txt: Id,NickName,JobTitle,...
                pageId.set(Integer.parseInt(parts[0].trim()));
                outValue.set(
                    "I," + parts[1].trim() + "," + parts[2].trim()); // I = info
                context.write(pageId, outValue);
            }
        }
    }

    // Reducer for job 2
    public static class Top10Reducer
        extends Reducer<IntWritable, Text, Text, NullWritable> {
        private List<PageRecord> pageList = new ArrayList<>();

        // Helper class to store page info + count
        private static class PageRecord {
            int pageId;
            String nick;
            String title;
            int count;

            PageRecord(int pageId, String nick, String title, int count) {
                this.pageId = pageId;
                this.nick = nick;
                this.title = title;
                this.count = count;
            }
        }

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
            String nick = "UNKNOWN";
            String title = "UNKNOWN";
            int count = 0;

            for (Text t : values) {
                String s = t.toString();
                if (s.startsWith("C,")) {
                    count = Integer.parseInt(s.substring(2));
                } else if (s.startsWith("I,")) {
                    String[] f = s.substring(2).split(",", 2);
                    nick = f[0];
                    title = f[1];
                }
            }

            pageList.add(new PageRecord(key.get(), nick, title, count));
        }

        @Override
        protected void cleanup(Context context)
            throws IOException, InterruptedException {
            // Sort descending by count
            pageList.sort((a, b) -> Integer.compare(b.count, a.count));

            // Output w/o the count
            for (int i = 0; i < Math.min(10, pageList.size()); i++) {
                PageRecord p = pageList.get(i);
                context.write(new Text(p.pageId + "," + p.nick + "," + p.title),
                    NullWritable.get());
            }
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {
        // -------------------- JOB 1 --------------------
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
            job1, new Path("file:///home/ds503/ActivityLog.txt"));

        FileOutputFormat.setOutputPath(job1,
            new Path("file:///home/ds503/shared_folder/project1/taskB/temp"));

        // 7. submit the job
        boolean result1 = job1.waitForCompletion(true);

        if (!result1) {
            System.exit(1);
        }

        // -------------------- JOB 2 --------------------
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
        job2.setMapOutputValueClass(Text.class);

        // 5. set up the final output key value data type class (It doesn't have to be the result of a reducer.)
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        // 6. Specify the input and output path
        FileInputFormat
            .setInputPaths(job2,
                new Path("file:///home/ds503/shared_folder/project1/taskB/"
                         + "temp"), // Job1 counts
                new Path("file:///home/ds503/CircleNetPage.txt") // page info
            );

        FileOutputFormat.setOutputPath(job2,
            new Path("file:///home/ds503/shared_folder/project1/taskB/output"));

        // 7. submit the job
        boolean result2 = job2.waitForCompletion(true);
        System.exit(result2 ? 0 : 1);
    }
}
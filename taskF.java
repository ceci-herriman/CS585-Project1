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

// those who have more followers than the average number of followers across all
// owners of a CircleNetPage

/*
scp -P 14226 C:/Users/op902/CS585-Project1/taskF.java ds503@localhost:~/

compile and run instrutions I used:
javac -classpath $(hadoop classpath) taskF.java
jar cf taskf.jar taskF*.class
rm -rf ~/shared_folder/project1/taskF/output
hadoop jar taskf.jar taskF

cat ~/shared_folder/project1/taskF/output/part-r-00000
*/
public class taskF {
    // Mapper
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
                    int pageOwnerId =
                        Integer.parseInt(fields[2].trim()); // id2 = page owner
                    ownerId.set(pageOwnerId);
                    context.write(ownerId, ONE);
                } catch (NumberFormatException e) {
                    // skip bad lines
                }
            }
        }
    }

    // Combiner
    public static class SumCombiner
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

    // Reducer (Optimized)
    public static class AboveAverageReducer
        extends Reducer<IntWritable, IntWritable, Text, IntWritable> {
        private Map<Integer, Integer> counts = new HashMap<>();
        private int totalFollowers = 0;
        private int totalOwners = 0;
        private Map<Integer, String> ownerInfo = new HashMap<>();

        @Override
        protected void setup(Context context)
            throws IOException, InterruptedException {
            // Load CircleNetPage.txt from Distributed Cache
            BufferedReader br =
                new BufferedReader(new FileReader("CircleNetPage.txt"));

            String line;
            while ((line = br.readLine()) != null) {
                String[] f = line.split(",");
                if (f.length >= 3) {
                    ownerInfo.put(Integer.parseInt(f[0].trim()),
                        f[1].trim() + "," + f[2].trim());
                }
            }
            br.close();
        }

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }

            counts.put(key.get(), sum);
            totalFollowers += sum;
            totalOwners++;
        }

        @Override
        protected void cleanup(Context context)
            throws IOException, InterruptedException {
            double avg = (double) totalFollowers / totalOwners;

            for (Map.Entry<Integer, Integer> e : counts.entrySet()) {
                if (e.getValue() > avg) {
                    String info =
                        ownerInfo.getOrDefault(e.getKey(), "UNKNOWN,UNKNOWN");

                    context.write(new Text(e.getKey() + "," + info),
                        new IntWritable(e.getValue()));
                }
            }
        }
    }

    // Reducer (Simple)
    /*
    public static class AboveAverageReducer
        extends Reducer<IntWritable, IntWritable, Text, IntWritable> {

        private Map<Integer, Integer> counts = new HashMap<>();
        private int totalFollowers = 0;
        private int totalOwners = 0;

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            counts.put(key.get(), sum);
            totalFollowers += sum;
            totalOwners++;
        }

        @Override
        protected void cleanup(Context context)
            throws IOException, InterruptedException {
            double avg = (double) totalFollowers / totalOwners;

            Map<Integer, String> ownerInfo = new HashMap<>();
            BufferedReader br =
                new BufferedReader(new FileReader("CircleNetPage.txt")); //CircleNetPage.txt has owner info

            String line;
            while ((line = br.readLine()) != null) {
                String[] f = line.split(",");
                if (f.length >= 3) {
                    ownerInfo.put(Integer.parseInt(f[0].trim()),
                        f[1].trim() + "," + f[2].trim());
                }
            }
            br.close();

            // output owners above average
            for (Map.Entry<Integer, Integer> e : counts.entrySet()) {
                if (e.getValue() > avg) {
                    String info =
                        ownerInfo.getOrDefault(e.getKey(), "UNKNOWN,UNKNOWN");
                    context.write(new Text(e.getKey() + "," + info),
                        new IntWritable(e.getValue()));
                }
            }
        }
    }*/

    
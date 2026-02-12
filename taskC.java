import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import javax.naming.Context;

//compile and run instrutions I used: 
// javac -classpath $(hadoop classpath) taskC.java
// jar cf taskC.jar taskC*.class
// rm -rf ~/shared_folder/project1/taskC/output
// hadoop jar taskC.jar taskC

//Time results w/ opt 3022.652918, 2947.577585, 1980.261251, 2979.899251, 2968.197626
//Time results w/ simple 3008.784126, 3019.047751, 3113.44771, 2985.10446, 4072.265794

public class taskC {
    
    //for optimal solution
    public static class taskCMapperOpt extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] vals = line.split(",");

            //each line has the format: f'{id},{userNickname},{userJob},{userRegionCode},{userHobby}
            if(vals[4].equals("Hockey")) {
                //we return the user details 
                String userNickname = vals[1];
                String jobTitle = vals[2];
                String hobby = vals[4];
                context.write(new Text(hobby), new Text(userNickname + " " + jobTitle));
            }
        }
    }

    //for simple solution
    public static class taskCMapper extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] vals = line.split(",");

            //each line has the format: f'{id},{userNickname},{userJob},{userRegionCode},{userHobby}
            String userNickname = vals[1];
            String jobTitle = vals[2];
            String hobby = vals[4];

            context.write(new Text(hobby), new Text(userNickname + " " + jobTitle));
        }
    }

     public static class taskCReducer extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String hobby = key.toString(); 
            
            if(hobby.equals("Hockey")) {
                for(Text val : values) {
                    String[] s = val.toString().split(" ");
                    context.write(new Text(hobby), new Text(s[0] + "-" + s[1]));
                }
            }

        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // job.setReduceSpeculativeExecution(false);

        job.setJarByClass(taskC.class);

        //optimized version:
        job.setMapperClass(taskCMapperOpt.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        //simple version
        // job.setMapperClass(taskCMapper.class);
        // job.setReducerClass(taskCReducer.class);

        // job.setMapOutputKeyClass(Text.class);
        // job.setMapOutputValueClass(Text.class);

        // job.setOutputKeyClass(Text.class);
        // job.setOutputValueClass(Text.class);
        
        FileInputFormat.setInputPaths(job, new Path("file:///home/ds503/shared_folder/project1/taskC/input/CircleNetPage.txt"));
        FileOutputFormat.setOutputPath(job, new Path("file:///home/ds503/shared_folder/project1/taskC/output"));

        long startTime = System.nanoTime();
        boolean result = job.waitForCompletion(true);
        long endTime = System.nanoTime();

        double durationMilli = (double) (endTime - startTime) / 1000000.0;
        System.out.println("Time to complete in milliseconds: " + durationMilli);

        System.exit(result ? 0 : 1);
    }
}

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import javax.naming.Context;

//compile and run instrutions I used: 
// javac -classpath $(hadoop classpath) taskC.java
// jar cf taskc.jar taskC*.class
// rm -rf ~/shared_folder/project1/output -- hadoop won't override I don't think 
// hadoop jar taskc.jar taskC

public class taskC {
    
    public static class taskCMapper extends Mapper<Object, Text, Text, Text>{

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
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1. create a job object
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2. map the class
        job.setJarByClass(taskC.class);

        // 3. both the mapper class
        job.setMapperClass(taskCMapper.class);

        // 4. set up the output key value data type class
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 5. set up the final output key value data type class (It doesn't have to be the result of a reducer.)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 6. Specify the input and output path
        FileInputFormat.setInputPaths(job, new Path("file:///home/ds503/shared_folder/project1/taskC/input/CircleNetPage.txt"));
        FileOutputFormat.setOutputPath(job, new Path("file:///home/ds503/shared_folder/project1/taskC/output"));

        // 7. submit the job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}

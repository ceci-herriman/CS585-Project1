package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1. create a job object
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2. map the class
        job.setJarByClass(WordCountDriver.class);

        // 3. map both the mapper and reducer class
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4. set up the output key value data type class
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5. set up the final output key value data type class (It doesn't have to be the result of a reducer.)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6. Specify the input and output path
        FileInputFormat.setInputPaths(job, new Path("D:\\sgg_tutorial\\MapReduceDemo\\MapReduceDemo\\data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\sgg_tutorial\\MapReduceDemo\\MapReduceDemo\\output"));

        // 7. submit the job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}

package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN, the data type of input key of map phase：LongWritable
 * VALUEIN, the data type of input value of map phase： Text
 * KEYOUT, the data type of output key of map phase：Text
 * VALUEOUT, the data type of output value of map phase：IntWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outK = new Text(); // Create a Text object for output key
    private IntWritable outV = new IntWritable(1); // Create a IntWritable for output value

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 1. get a line at a time from the input data file
        String line = value.toString();

        // 2. divide the line we got from step 1 into words by specifying the text delimiter " "
        String[] words = line.split(" ");

        // 3. use for loop to loop each individual word from word variable
        for (String word : words) {
            // wrap the value of word into the output key object outK
            outK.set(word);
            // write out each (key,value) pair of map phase
            context.write(outK, outV);
        }

    }
}

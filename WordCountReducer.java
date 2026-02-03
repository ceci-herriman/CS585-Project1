package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * KEYIN, the data type of input key of reduce phase：Text (it matches the data type of output key of map phase)
 * VALUEIN, the data type of input value of reduce phase：IntWritable (it matches the data type of output value of map phase)
 * KEYOUT, the data type of output key of reduce phase：Text
 * VALUEOUT, the data type of output value of reduce phase：IntWritable
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    private IntWritable outV = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        // get the total count of word occurrences for each word (input key)
        for (IntWritable value : values) {
            sum += value.get(); // The data type of value is already IntWritable, to get its value, we need to call a getter
        }
        outV.set(sum);

        // write out the result (output (key, value) pair of reduce phase)
        context.write(key,outV);
    }
}

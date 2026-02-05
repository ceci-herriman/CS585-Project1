import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

public class CountAccessMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private final static IntWritable ONE = new IntWritable(1);
    private IntWritable pageId = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length == 5) {
            pageId.set(Integer.parseInt(fields[2])); // WhatPage
            context.write(pageId, ONE);
        }
    }
}

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.Context;

//compile and run instrutions I used: 
// javac -classpath $(hadoop classpath) taskH.java
// jar cf taskH.jar taskH*.class
// rm -rf ~/shared_folder/project1/taskH/output -- hadoop won't override I don't think 
// hadoop jar taskh.jar taskH

public class taskH {
    
    //parse regions
    public static class taskHMapper1 extends Mapper<Object, Text, IntWritable, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] vals = line.split(",");

            //each line has the format: f'{id},{userNickname},{userJob},{userRegionCode},{userHobby}
            Integer id = Integer.parseInt(vals[0]);
            Integer region = Integer.parseInt(vals[3]);
            String nickname = vals[1];
            context.write(new IntWritable(id), new Text("REGION:" + region + "NICKNAME:" + nickname));
        }
    }

    //parse follows
    public static class taskHMapper2 extends Mapper<Object, Text, IntWritable, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] vals = line.split(",");

            //each line has the format: f'{colId},{id1},{id2},{date},{desc}
            Integer id1 = Integer.parseInt(vals[1]);
            Integer id2 = Integer.parseInt(vals[2]);

            context.write(new IntWritable(id1), new Text("FOLLOWS:" + id2));
            context.write(new IntWritable(id2), new Text("FOLLOWED BY:" + id1));
            
        }
    }




    //////SIMPLE SOLUTION:
    public static class taskHReducer1 extends Reducer<IntWritable,Text,Text,Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Integer region = 0;
            String nickname = "";
            Integer user = key.get(); 
            List<Integer> followsList = new ArrayList<>();
            List<Integer> followedList = new ArrayList<>();

            for (Text val : values) {
                String s = val.toString();
                if (s.startsWith("REGION:")) {
                    Integer endIndex = s.indexOf("NICKNAME"); 
                    region = Integer.parseInt(s.substring(7, endIndex));
                    nickname = s.substring(endIndex + 9);
                } else if (s.startsWith("FOLLOWS:")) {
                    followsList.add(Integer.parseInt(s.substring(8)));
                } else if (s.startsWith("FOLLOWED BY:")) {
                    followedList.add(Integer.parseInt(s.substring(12)));
                }
            }

            if (region != 0) {
                for (Integer f : followsList) {
                    context.write(new Text(user + "," + f), new Text("FOLLOWER_DATA:" + region + "," + nickname));
                }

                for (Integer f : followedList) {
                    context.write(new Text(f + "," + user), new Text("FOLLOWED_DATA:" + region));
                }
            }
        }
  }

    //pass through to reducer
    public static class taskHMapper3 extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t", 2);
            
            if (parts.length == 2) {
                Text outputKey = new Text(parts[0]);
                Text outputValue = new Text(parts[1]);
                context.write(outputKey, outputValue);
            }
        }
    }

    //combine based on region and remove duplicate following pairs
    public static class taskHReducer2 extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] pair = key.toString().split(",");
            String follower_id = pair[0];
            String followed_id = pair[1];

            String follower_region = "0"; 
            String follower_nickname = "";
            String followed_region = "0";

            for (Text val : values) { //taking the format of ""FOLLOWS/FOLLOWED BY:" region (nickname)"
                String s = val.toString(); 

                if(s.startsWith("FOLLOWER_DATA")) {

                    Integer endIndex = s.indexOf(",");
                    follower_region = s.substring(14, endIndex);
                    follower_nickname = s.substring(endIndex + 1);

                } else if(s.startsWith("FOLLOWED_DATA")) {
                    followed_region = s.substring(14);
                }
                
            }
            //check regions and pass forward if they match 
            if(followed_region.equals(follower_region) && !followed_region.equals("0")) {
                context.write(new Text(follower_id + "," + followed_id), new Text(follower_nickname));
            }
        }
    }

    //sort (uid, followed_id)
    public static class taskHMapper4 extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t", 2);
            
            if (parts.length == 2) {
                String[] tuple = parts[0].toString().split(",");
                String nickname = parts[1].toString();

                Integer uid = Integer.parseInt(tuple[0]);
                Integer fdid = Integer.parseInt(tuple[1]);

                if(uid <= fdid) {
                    context.write(new Text(uid + "," + fdid), new Text("FOLLOWER:" + uid + "," + nickname));
                } 
                else {
                    context.write(new Text(fdid + "," + uid), new Text("FOLLOWER:" + uid + "," + nickname));
                }
            }
        }
    }

    //combine based on (id, id) and return duplicates
    public static class taskHReducer3 extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Integer numVals = 0;
            String val = "";

            for(Text str : values) {
                String s = str.toString();
                val = s;
                numVals = numVals + 1;
            }

            if(numVals == 1) {
                //only one occurence, so not a mutual following
                Integer firstIndex = val.indexOf(":");
                Integer secondIndex = val.indexOf(",");
                context.write(new Text(val.substring(firstIndex + 1, secondIndex)), new Text(val.substring(secondIndex + 1)));
            }

        }
    }

    //map forward to remove duplicates 
    public static class taskHMapper5 extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t", 2);
            context.write(new Text(parts[0]), new Text(parts[1]));
        }
    }

    //remove duplicates 
    public static class taskHReducer4 extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Integer numVals = 0;
            String val = "";

            for(Text str : values) {
                String s = str.toString();
                val = s;
                numVals = numVals + 1;
            }

            context.write(new Text(key.toString()), new Text(val));
        }
    }




    //////OPTIMIZED SOLUTION:
    public static class taskHReducerOp1 extends Reducer<IntWritable,Text,Text,Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Integer region = 0;
            String nickname = "";
            Integer user = key.get(); 
            List<Integer> followsList = new ArrayList<>();
            List<Integer> followedList = new ArrayList<>();

            for (Text val : values) {
                String s = val.toString();
                if (s.startsWith("REGION:")) {
                    Integer endIndex = s.indexOf("NICKNAME"); 
                    region = Integer.parseInt(s.substring(7, endIndex));
                    nickname = s.substring(endIndex + 9);
                } else if (s.startsWith("FOLLOWS:")) {
                    followsList.add(Integer.parseInt(s.substring(8)));
                } else if (s.startsWith("FOLLOWED BY:")) {
                    followedList.add(Integer.parseInt(s.substring(12)));
                }
            }
            

            //write out
            if (region != 0) {
                for (Integer f : followsList) {
                    
                    //sort user ids ascending
                    Integer id1 = 0; 
                    Integer id2 = 0;

                    if(user > f) {
                        id1 = f;
                        id2 = user; 
                    }
                    else {
                        id1 = user; 
                        id2 = f;
                    }
                    //user follows f
                    context.write(new Text(id1 + "," + id2), new Text("FOLLOWER_DATA:" + user + ","+ region + " " + nickname));
                }

                for (Integer f : followedList) {
                    
                    //sort user ids ascending
                    Integer id1 = 0; 
                    Integer id2 = 0;

                    if(user > f) {
                        id1 = f;
                        id2 = user; 
                    }
                    else {
                        id1 = user; 
                        id2 = f;
                    }
                    //f folows user
                    context.write(new Text(id1 + "," + id2), new Text("FOLLOWED_DATA:" + user + "," + region));
                }
            }
        }
    
    }
    
    public static class taskHMapperOp2 extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t", 2);
            
            if (parts.length == 2) {
                Text outputKey = new Text(parts[0]);
                Text outputValue = new Text(parts[1]);
                context.write(outputKey, outputValue);
            }
        }
    }

    public static class taskHReducerOp2 extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String follower_region = "0"; 
            String follower_nickname = "";
            String follower_id = "";
            String followed_id = "";
            String followed_region = "0";

            //get number of tuples -- 4 if mutual follow, 2 for one way follow
            Integer numVals = 0;
            String val = "";

            List<String> cachedVals = new ArrayList<>();

            for(Text str : values) {
                String s = str.toString();
                val = s;
                numVals = numVals + 1;
                cachedVals.add(s);
            }

            if(numVals == 2) {
                //then we check if regions are the same and write out if so
                for (int i = 0; i < cachedVals.size(); i++) { //taking the format of ""FOLLOWER_DATA/FOLLOWED_DATA:id,region (nickname)"
                    String s = cachedVals.get(i);

                    if(s.startsWith("FOLLOWER_DATA")) {
                        Integer firstIndex = s.indexOf(",");
                        follower_id = s.substring(14, firstIndex);
                        Integer secondIndex = s.indexOf(" ");
                        follower_region = s.substring(firstIndex + 1, secondIndex);
                        follower_nickname = s.substring(secondIndex + 1);

                    } else if(s.startsWith("FOLLOWED_DATA")) {
                        Integer firstIndex = s.indexOf(",");
                        followed_id = s.substring(14, firstIndex);
                        followed_region = s.substring(firstIndex + 1);
                    }
                }

                if (followed_region.equals(follower_region) && !followed_region.equals("0")) {
                    context.write(new Text(follower_id), new Text(follower_nickname));
                }
            }
        }
    }

    //map forward to remove duplicates 
    public static class taskHMapperOp3 extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t", 2);
            context.write(new Text(parts[0]), new Text(parts[1]));
        }
    }

    //remove duplicates 
    public static class taskHReducerOp3 extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Integer numVals = 0;
            String val = "";

            for(Text str : values) {
                String s = str.toString();
                val = s;
                numVals = numVals + 1;
                break;
            }

            context.write(new Text(key.toString()), new Text(val));
        }
    }





    /////main run functions

    //simple version main() -- rename to main() to run the simple version
    public static void mainX(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // first job
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf);

        job1.setJarByClass(taskH.class);

        //mappers connected to respective input files
        MultipleInputs.addInputPath(job1, new Path("file:///home/ds503/shared_folder/project1/taskH/input/CircleNetPage.txt"), TextInputFormat.class, taskHMapper1.class);
        MultipleInputs.addInputPath(job1, new Path("file:///home/ds503/shared_folder/project1/taskH/input/Follows.txt"), TextInputFormat.class, taskHMapper2.class);

        job1.setReducerClass(taskHReducer1.class);

        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job1, new Path("file:///home/ds503/shared_folder/project1/taskH/intermediate"));

        long startTime = System.nanoTime();
        
        boolean job1Result = job1.waitForCompletion(true);
        
        if (!job1Result) {
            System.exit(1);
        }

        // second job
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2);

        job2.setJarByClass(taskH.class);

        job2.setMapperClass(taskHMapper3.class);
        job2.setReducerClass(taskHReducer2.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job2, new Path("file:///home/ds503/shared_folder/project1/taskH/intermediate"));
        FileOutputFormat.setOutputPath(job2, new Path("file:///home/ds503/shared_folder/project1/taskH/intermediate2"));

        boolean job2Result = job2.waitForCompletion(true);

        if (!job2Result) {
            System.exit(1);
        }

        // third job
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3);

        job3.setJarByClass(taskH.class);

        job3.setMapperClass(taskHMapper4.class);
        job3.setReducerClass(taskHReducer3.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job3, new Path("file:///home/ds503/shared_folder/project1/taskH/intermediate2"));
        FileOutputFormat.setOutputPath(job3, new Path("file:///home/ds503/shared_folder/project1/taskH/intermediate3"));

        boolean job3Result = job3.waitForCompletion(true);

        if (!job3Result) {
            System.exit(1);
        }

        // fourth job
        Configuration conf4 = new Configuration();
        Job job4 = Job.getInstance(conf4);

        job4.setJarByClass(taskH.class);

        job4.setMapperClass(taskHMapper5.class);
        job4.setReducerClass(taskHReducer4.class);

        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);

        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job4, new Path("file:///home/ds503/shared_folder/project1/taskH/intermediate3"));
        FileOutputFormat.setOutputPath(job4, new Path("file:///home/ds503/shared_folder/project1/taskH/output"));

        boolean job4Result = job4.waitForCompletion(true);

        long endTime = System.nanoTime();
        double durationMilli = (double) (endTime - startTime) / 1000000.0;
        System.out.println("Time to complete in milliseconds: " + durationMilli);

        System.exit(job4Result ? 0 : 1);
    }

    //optimized version main() -- rename to main() to run the optimized version
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // first job
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf);

        job1.setJarByClass(taskH.class);

        //mappers connected to respective input files
        MultipleInputs.addInputPath(job1, new Path("file:///home/ds503/shared_folder/project1/taskH/input/CircleNetPage.txt"), TextInputFormat.class, taskHMapper1.class);
        MultipleInputs.addInputPath(job1, new Path("file:///home/ds503/shared_folder/project1/taskH/input/Follows.txt"), TextInputFormat.class, taskHMapper2.class);

        job1.setReducerClass(taskHReducerOp1.class);

        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job1, new Path("file:///home/ds503/shared_folder/project1/taskH/intermediate"));

        long startTime = System.nanoTime();
        
        boolean job1Result = job1.waitForCompletion(true);
        
        if (!job1Result) {
            System.exit(1);
        }

        // second job
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2);

        job2.setJarByClass(taskH.class);

        job2.setMapperClass(taskHMapperOp2.class);
        job2.setReducerClass(taskHReducerOp2.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job2, new Path("file:///home/ds503/shared_folder/project1/taskH/intermediate"));
        FileOutputFormat.setOutputPath(job2, new Path("file:///home/ds503/shared_folder/project1/taskH/intermediate2"));

        boolean job2Result = job2.waitForCompletion(true);

        if (!job2Result) {
            System.exit(1);
        }

        // third job
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3);

        job3.setJarByClass(taskH.class);

        job3.setMapperClass(taskHMapperOp3.class);
        job3.setReducerClass(taskHReducerOp3.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job3, new Path("file:///home/ds503/shared_folder/project1/taskH/intermediate2"));
        FileOutputFormat.setOutputPath(job3, new Path("file:///home/ds503/shared_folder/project1/taskH/output"));

        boolean job3Result = job3.waitForCompletion(true);

        long endTime = System.nanoTime();
        double durationMilli = (double) (endTime - startTime) / 1000000.0;
        System.out.println("Time to complete in milliseconds: " + durationMilli);

        System.exit(job3Result ? 0 : 1);
    }
}

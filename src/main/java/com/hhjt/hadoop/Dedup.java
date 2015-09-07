package com.hhjt.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by paul on 2015/6/25.
 */
public class Dedup {
    public static class Map extends Mapper<Object, Text, Text,Text>{
        private static Text line = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            line = value;
            context.write(line,new Text(""));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key,new Text(""));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//        String[] otherAgs = new GenericOptionsParser(conf,args).getRemainingArgs();
//        if (otherAgs.length !=2 ){
//            System.err.println("Usage: ");
//        }
        Job job = new Job(conf,"Data Deduplication");
        job.setJarByClass(Dedup.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}

package com.hhjt.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by paul on 2015/6/25.
 */
public class Sort {
    public static class Map extends Mapper<Object, Text, IntWritable, IntWritable>{
        private static IntWritable data = new IntWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            data.set(Integer.parseInt(line));
            context.write(data,new IntWritable(1));
        }
    }

    public static class Reduce extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{

        private static IntWritable linenum = new IntWritable(1);
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable val:values){
                context.write(linenum,key);
                linenum = new IntWritable(linenum.get()+1);
            }
        }
    }

    public static class Partition extends Partitioner<IntWritable,IntWritable>{
        @Override
        public int getPartition(IntWritable key, IntWritable vauel, int numPartitions) {
            int maxNumber = 65233;
            int bound = maxNumber/numPartitions + 1;
            int keyNumber = key.get();
            for (int i = 0; i < numPartitions; i++){
                if (keyNumber < bound * (i+1) && keyNumber >= bound * i){
                    return i;
                }
            }
            return -1;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Sort");
//        job.setNumReduceTasks();
        job.setJarByClass(Sort.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setPartitionerClass(Partition.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean b = job.waitForCompletion (true);
        System.exit(b ? 0 : 1);
    }
}

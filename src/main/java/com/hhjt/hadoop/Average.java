package com.hhjt.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by paul on 2015/6/24.
 */
public class Average extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Average(),args);
        System.exit(ret);
    }
    @Override
    public int run(String[] strings) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(Average.class);
        job.setJobName("score_process");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(TokenizeMap.class);
        job.setReducerClass(TokenizeReduce.class);
        job.setCombinerClass(TokenizeReduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job,new Path(strings[0]));
        FileOutputFormat.setOutputPath(job,new Path(strings[1]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static class TokenizeMap extends Mapper<LongWritable, Text, Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            System.out.println(value);
            StringTokenizer tokenizerArticle = new StringTokenizer(line,"\n");
            while (tokenizerArticle.hasMoreTokens()){
                StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken());
                String strName = tokenizerLine.nextToken();
                Text name = new Text(strName);
                while (tokenizerLine.hasMoreTokens()){
                    String strScore = tokenizerLine.nextToken();
                    int score = Integer.valueOf(strScore);
                    context.write(name,new IntWritable(score));
                }


            }
        }
    }

    public static class TokenizeReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            Iterator<IntWritable> iterator = values.iterator();
            while (iterator.hasNext()){
                sum += iterator.next().get();
                count++;
            }
            int ave = sum/count;
            context.write(key,new IntWritable(ave));
        }
    }
}

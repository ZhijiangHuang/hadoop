package com.hhjt.hadoop.mrunit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by root on 2015-8-7.
 */
public class SMSCDRMapperReducerTest {
    HashMap map;
    Configuration conf = new Configuration();
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        System.setProperty("hadoop.home.dir", "d:/hadoop-2.6.0");

        //测试mapreduce
        SMSCDRMapper mapper = new SMSCDRMapper();
        SMSCDRReducer reducer = new SMSCDRReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

        //测试配置参数
        mapDriver.setConfiguration(conf);
        conf.set("myParameter1", "20");
        conf.set("myParameter2", "23");

    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(
                "655209;1;796764372490213;804422938115889;6"));
        mapDriver.withOutput(new Text("6"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("6"), values);
        reduceDriver.withOutput(new Text("6"), new IntWritable(2));
        reduceDriver.runTest();
    }

    @Test
    public void testMapperReducer() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "655209;1;796764372490213;804422938115889;6"));
        mapReduceDriver.withOutput(new Text("6"), new IntWritable(1));
    }

//    @Test
//    public void testMapperCount() throws IOException {
//        mapDriver.withInput(new LongWritable(), new Text(
//                "655209;0;796764372490213;804422938115889;6"));
//        // mapDriver.withOutput(new Text("6"), new IntWritable(1));
//        mapDriver.runTest();
//        assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()
//                .findCounter(CDRCounter.NonSMSCDR).getValue());
//    }
}

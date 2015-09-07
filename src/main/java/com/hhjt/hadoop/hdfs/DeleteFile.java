package com.hhjt.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * Created by root on 2015-8-4.
 */
public class DeleteFile {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        String src = "hdfs://172.16.1.41:9000/user/root/output3";
        FileSystem fileSystem = FileSystem.get(URI.create(src),conf);
        fileSystem.delete(new Path(src),true);
    }
}

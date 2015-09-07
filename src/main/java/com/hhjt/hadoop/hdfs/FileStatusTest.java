package com.hhjt.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * Created by root on 2015-8-4.
 */
public class FileStatusTest {
    public static void main(String[] args) throws IOException {
        String src = "hdfs://172.16.1.41:9000/user/root/input/file01";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(src),conf);
        Path path = new Path(src);

        FileStatus fileStatus = fs.getFileStatus(path);
        System.out.println("文件路径："+ fileStatus.getPath());
        System.out.println("block size:" + fileStatus.getBlockSize());
        System.out.println("owner:" + fileStatus.getOwner() + ":" + fileStatus.getGroup());
        System.out.println(fileStatus.getLen());
    }
}

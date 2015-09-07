package com.hhjt.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

/**
 * Created by root on 2015-8-4.
 */
public class FileCopyWithProgress {
    public static void main(String[] args) throws IOException {
        String localSrc = "e:\\test.txt";
        String dst = "hdfs://172.16.1.41:9000/user/root/test/test.txt";
        InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
        System.out.println(in.read());
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst),configuration);
        OutputStream out = fs.create(new Path(dst), new Progressable() {
            @Override
            public void progress() {
                System.out.print("*");
            }
        });
        IOUtils.copyBytes(in,out,in.read(),true);
    }
}

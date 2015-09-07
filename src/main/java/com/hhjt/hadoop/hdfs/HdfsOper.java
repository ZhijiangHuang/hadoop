package com.hhjt.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

/**
 * Created by root on 2015-9-7.
 */
public class HdfsOper {
    private FileSystem fileSystem;
    private Configuration configuration;
    public HdfsOper() throws IOException {
        configuration = new Configuration();
        fileSystem = FileSystem.get(configuration);
    }
    public void write(String localFile,String hdfsPath) throws IOException {
        InputStream inputStream = new BufferedInputStream(new FileInputStream(localFile));
        FileSystem fileSystem2 = FileSystem.get(URI.create(hdfsPath), configuration);
        OutputStream outputStream =  fileSystem2.create(new Path(hdfsPath));
        IOUtils.copyBytes(inputStream,outputStream,configuration);
    }

    public static void main(String[] args) throws IOException {
        String localFile = "e:/a.txt";
        String hdfsPath = "hdfs://172.16.1.41:9000/user/root/input/a.txt";
        HdfsOper oper = new HdfsOper();
        oper.write(localFile,hdfsPath);
    }
}

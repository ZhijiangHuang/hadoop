package com.hhjt.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * Created by root on 2015-8-4.
 */
public class FileSystemCat {
    public static void main(String[] args) throws IOException {
        String uri = "hdfs://172.16.1.41:9000/user/root/test/test.txt";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri),conf);
        FSDataInputStream in = null;
        BufferedReader br = null;
        try {
            in = fs.open(new Path(uri));

            br = new BufferedReader(new InputStreamReader(in));
            String line = "";
            while ((line=br.readLine())!=null){
                System.out.println(line);
            }
            //            while (in)
//            IOUtils.copyBytes(in,System.out,in.read(),false);
//            in.seek(3);
//            IOUtils.copyBytes(in,System.out,4096,false);

        }finally {
            in.close();
            br.close();
//            IOUtils.closeStream(in);
        }


    }
}

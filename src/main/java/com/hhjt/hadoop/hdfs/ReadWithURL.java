package com.hhjt.hadoop.hdfs;


import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * Created by paul on 2015-8-3.
 */
public class ReadWithURL {
    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }
    public static void main(String[] args) {
        InputStream in = null;
        try {
            in = new URL("hdfs://172.16.1.41:9000/user/root/input/file01").openStream();
            IOUtils.copyBytes(in, System.out, 4096, false);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(in);
        }
    }
}

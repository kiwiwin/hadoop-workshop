package com.github.kiwiwin.hadoop.hdfsbasic;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Scanner;

/**
 * NOTE: need to specify the handler factory in order to parse the URL
 *
 * This Small Demo needs
 * compile "org.apache.hadoop:hadoop-client:$hadoopVersion"
 *
 */
public class HdfsBasic {
    public static void main(String[] args) throws Exception {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());

        InputStream inputStream = null;
        try {
            inputStream = new URL("hdfs://localhost/input1.txt").openStream();

            final Scanner scanner = new Scanner(inputStream);

            final String line = scanner.next();

            System.out.println(line);
            scanner.close();
            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

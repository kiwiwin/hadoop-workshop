package com.github.kiwiwin.hadoop.hdfsbasic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Scanner;

public class HdfsApiBasic {
    public static void main(String[] args) throws Exception {
        final String uri = "hdfs://localhost/input";
        final Configuration configuration = new Configuration();

        final FileSystem fs = FileSystem.get(URI.create(uri), configuration);

        InputStream inputStream = null;
        try {
            inputStream = fs.open(new Path(uri));

            final Scanner scanner = new Scanner(inputStream);

            final String line = scanner.next();

            System.out.println(line);
            scanner.close();
            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
    }
}

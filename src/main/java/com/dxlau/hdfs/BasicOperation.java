package com.dxlau.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

/**
 * HDFS基本操作.
 * <p>
 * 设置HADOOP_USER_NAME环境变量或系统属性，注意权限异常.
 */
public class BasicOperation {

    private FileSystem fileSystem;

    public BasicOperation(Configuration conf) throws IOException {
        this.fileSystem = FileSystem.get(conf);
    }

    public void ls(String url) throws IOException {
        Path path = new Path(url);
        RemoteIterator<LocatedFileStatus> iter = this.fileSystem.listFiles(path, false);
        while (iter.hasNext()) {
            LocatedFileStatus fileStatus = iter.next();
            System.out.println(fileStatus.getPath());
        }
    }

    public void write(String url) throws IOException {
        Path path = new Path(url);
        FSDataOutputStream stream = this.fileSystem.create(path, true);

        // Dept
        String fmt = "%d\t%s\n";
        stream.writeBytes(String.format(fmt, 2, "Marking"));
        stream.writeBytes(String.format(fmt, 3, "Finance"));
        stream.writeBytes(String.format(fmt, 5, "Sales"));
    }

    public void read(String url) throws IOException {
        Path path = new Path(url);

        FSDataInputStream stream = this.fileSystem.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(stream));
        String line;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }
    }

    public static void main(String[] args) throws IOException {
        String path = "/user/milk/dept/1.txt";
        Configuration conf = new Configuration();
        BasicOperation ops = new BasicOperation(conf);

//         ops.write(path);
         ops.read(path);
    }
}

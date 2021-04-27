package com.zhuge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author ZCH
 * @date 2021/4/19 0019 - 下午 3:08
 */
public class HdfsDemo {
    @Test
    public void ls() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop001:8020"), conf, "hadoop");
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus.getPath().getName());
        }
    }
    @Test
    public void  download() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop001:8020"),conf,"hadoop");
        fileSystem.copyToLocalFile(false,new Path("/test.txt"),new Path("E:\\IDEA-Projects\\hdfs\\src\\download"),true);
        fileSystem.close();
    }

    @Test
    public void upload() throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop001:8020"), conf, "hadoop");
        fileSystem.copyFromLocalFile(new Path("E:/IDEA-Projects/hdfs/src/upload/Hadoop笔记.md"),new Path("/"));
        fileSystem.close();
    }
    @Test
    public void delete() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop001:8020"), configuration, "hadoop");
        fileSystem.deleteOnExit(new Path("/hdfsClient.txt"));
    }
}

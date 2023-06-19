package com.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.shaded.org.jline.utils.InputStreamReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSTest {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        // 创建配置对象
        Configuration conf = new Configuration();
        // 设置数据节点主机名属性
        conf.set("dfs.client.use.datanode.hostname", "true");
        // 定义统一资源标识符 (uri: uniform resource identifier)
        String uri = "hdfs://namenode:8020";
        // 创建文件系统对象(基于HDFS的文件系统)
        FileSystem fs = FileSystem.get(new URI(uri), conf,  "root");

        // 创建路径对象 (指向文件)
        Path path = new Path( uri + "/students.json");

        // 创建文件系统数据字节输入流(进水管:数据从文件到程序)
        FSDataInputStream in = fs.open(path) ;

        // 创建缓冲字符输入流，提高读取效率(字节流-->字符流-->缓冲流)
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        // 定义行字符串变量
        String nextLine = "";
        // 通过循环遍历缓冲字符输入流
        while ((nextLine = br.readLine()) != null) {
            // 在控制台输出读取的行
            System.out.println(nextLine);
        }
    }
}

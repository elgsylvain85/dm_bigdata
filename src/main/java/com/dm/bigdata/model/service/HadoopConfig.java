package com.dm.bigdata.model.service;

import java.io.IOException;
import java.net.URI;

// import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HadoopConfig {

    @Value("${app.hadoop-namenode}")
    String hadoopNameNode;

    /**
     * Apache Spark Engin
     * 
     * @return
     * @throws IOException
     */
    @Bean
    public FileSystem hadoopFileSystem() throws IOException {

        var conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        FileSystem fs = FileSystem.get(URI.create(this.hadoopNameNode), conf);

        return fs;
    }

}

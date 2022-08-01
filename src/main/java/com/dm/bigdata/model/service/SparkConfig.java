package com.dm.bigdata.model.service;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Value("${app.name}")
    String appName;
    @Value("${app.spark-master}")
    String appSparkMaster;
    @Value("${app.hadoop-namenode}")
    String hadoopNameNode;
    @Value("${app.work-dir}")
    transient String appWorkDir;

    /**
     * Apache Spark Engin
     * 
     * @return
     */
    @Bean
    public SparkSession sparkSession() {

        return SparkSession.builder()
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.databricks.delta.properties.defaults.minWriterVersion", "5")// mandatory to support
                                                                                           // Column rename in Delta
                                                                                           // table
                .config("spark.databricks.delta.properties.defaults.minReaderVersion", "2")// mandatory to support
                                                                                           // Column rename in Delta
                                                                                           // table
                .config("spark.databricks.delta.properties.defaults.columnMapping.mode", "name")// mandatory to support
                                                                                                // Column rename in
                                                                                                // Delta table
                .config("spark.network.timeout", 10000000)
                .config("spark.sql.shuffle.partitions", 100)
                .config("spark.executor.heartbeatInterval", 10000000)
                .config("spark.sql.warehouse.dir", this.hadoopNameNode+this.appWorkDir+"/hive/warehouse")
                // .config("hive.metastore.warehouse.dir", this.hadoopNameNode+this.appWorkDir+"/hive/metastore")
                .appName(appName)
                .master(appSparkMaster)
                .enableHiveSupport()
                .getOrCreate();
    }

}

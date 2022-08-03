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
    String appWorkDir;

    @Value("${spark.network.timeout}")
    String sparkNetworkTimeout;
    @Value("${spark.sql.shuffle.partitions}")
    String sparkSqlShufflePartitions;
    @Value("${spark.executor.heartbeatInterval}")
    String sparkExecutorHeartbeatInterval;
    @Value("${spark.driver.memory}")
    String sparkDriverMemory;
    @Value("${spark.executor.memory}")
    String sparkExecutorMemory;

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
                .config("spark.network.timeout", Integer.valueOf(this.sparkNetworkTimeout))
                .config("spark.sql.shuffle.partitions", Integer.valueOf(this.sparkSqlShufflePartitions))
                .config("spark.executor.heartbeatInterval", Integer.valueOf(this.sparkExecutorHeartbeatInterval))
                .config("spark.sql.warehouse.dir", this.hadoopNameNode + this.appWorkDir + "/hive/warehouse")
                .config("spark.driver.memory", this.sparkDriverMemory)
                .config("spark.executor.memory", this.sparkExecutorMemory)
                // .config("hive.metastore.warehouse.dir",
                // this.hadoopNameNode+this.appWorkDir+"/hive/metastore")
                .appName(appName)
                .master(appSparkMaster)
                .enableHiveSupport()
                .getOrCreate();
    }

}

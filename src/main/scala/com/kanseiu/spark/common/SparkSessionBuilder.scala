package com.kanseiu.spark.common

import org.apache.spark.sql.SparkSession

object SparkSessionBuilder {

    def getOrCreateSparkSession(appName: String): SparkSession = {
        val sparkSession: SparkSession = SparkSession.builder
          .appName(appName)
          .config("spark.sql.warehouse.dir", Constants.sparkWarehouse)
          .config("hive.metastore.uris", Constants.metastoreUris)
          // 根据实际作业负载调整
          .config("spark.executor.memory", "512m")
          // 允许在没有指定的静态分区的时候创建一个分区
          .config("hive.exec.dynamic.partition.mode", "nonstrict")
          .config("hive.exec.dynamic.partition", "true")
          .enableHiveSupport()
          .getOrCreate()

        sparkSession
    }
}

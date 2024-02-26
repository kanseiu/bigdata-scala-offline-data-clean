package com.kanseiu.spark.handler

import com.kanseiu.spark.common.Constants
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object CustomerAddrLevelAggrDataClean {

    def main(args: Array[String]): Unit = {
        val tableName: String = "customer_addr_level_aggr"
        val dwdTableName: String = "dws.customer_addr_level_aggr"

        // 创建 sparkSession
        val sparkSession: SparkSession = SparkSession.builder
          .appName(s"$tableName offline data clean")
          .config("spark.sql.warehouse.dir", Constants.sparkWarehouse)
          .config("hive.metastore.uris", Constants.metastoreUris)
          .config("spark.executor.memory", "512m") // 根据需要设置 executor 内存
          .config("hive.exec.dynamic.partition.mode", "nonstrict")
          .enableHiveSupport()
          .getOrCreate()

        // 设置当前和昨天的日期
        val today = java.time.LocalDate.now()
        val yesterday = today.minusDays(1)
        val etlDate = yesterday.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"))
        val operationTimestamp = current_timestamp()

        // 读取Hive表数据
        val customerInf = sparkSession.table("dwd.dim_customer_inf")
        val customerAddr = sparkSession.table("dwd.dim_customer_addr")
        val customerLevelInf = sparkSession.table("dwd.dim_customer_level_inf")

        // 关联表数据
        val joinedData = customerInf
          .join(customerAddr, Seq("customer_id"), "left_outer")
          .join(customerLevelInf, Seq("customer_level"), "left_outer")

        // 添加额外的列
        val finalData = joinedData
          .withColumn("etl_date", lit(etlDate))
          .withColumn("dws_insert_user", lit("user1"))
          .withColumn("dws_insert_time", lit(operationTimestamp))
          .withColumn("dws_modify_user", lit("user1"))
          .withColumn("dws_modify_time", lit(operationTimestamp))

        // 写入Hive的DWS层
        finalData.write
          .mode(SaveMode.Overwrite)
          .partitionBy("etl_date")
          .saveAsTable(dwdTableName)

        sparkSession.stop()
    }
}
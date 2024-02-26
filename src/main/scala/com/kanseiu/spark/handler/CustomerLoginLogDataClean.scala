package com.kanseiu.spark.handler

import com.kanseiu.spark.common.Constants
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{current_timestamp, lit}

object CustomerLoginLogDataClean {

    def main(args: Array[String]): Unit = {
        val tableName: String = "customer_login_log"
        val odsTableName: String = "ods.customer_login_log"
        val dwdTableName: String = "dwd.log_customer_login"

        // 创建 sparkSession
        val sparkSession: SparkSession = SparkSession.builder
          .appName(s"$tableName offline data clean")
          .config("spark.sql.warehouse.dir", Constants.sparkWarehouse)
          .config("hive.metastore.uris", Constants.metastoreUris)
          .config("spark.executor.memory", "512m") // 根据需要设置 executor 内存
          .config("hive.exec.dynamic.partition.mode", "nonstrict")
          .enableHiveSupport()
          .getOrCreate()

        // 获取当前操作时间
        val operationTime = current_timestamp()

        // 从Hive的ODS层读取 odsTableName 表的最新分区数据
        val latestOdsPartition = sparkSession.sql(s"SELECT MAX(etl_date) FROM $odsTableName").collect()(0)(0).toString
        val odsData = sparkSession.sql(s"SELECT *, '$latestOdsPartition' as etl_date FROM $odsTableName WHERE etl_date='$latestOdsPartition'")

        // 添加新的列
        val transformedData = odsData
          .withColumn("dwd_insert_user", lit("user1"))
          .withColumn("dwd_insert_time", lit(operationTime))
          .withColumn("dwd_modify_user", lit("user1"))
          .withColumn("dwd_modify_time", lit(operationTime))

        // 写入到Hive的DWD层
        transformedData.write.mode(SaveMode.Overwrite).insertInto(dwdTableName)

        sparkSession.stop()
        
    }
}
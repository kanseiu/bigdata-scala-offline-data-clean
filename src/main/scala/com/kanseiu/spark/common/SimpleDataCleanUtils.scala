package com.kanseiu.spark.common

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{lit, current_timestamp}

object SimpleDataCleanUtils {

    def processTable(tableName: String, odsTableName: String, dwdTableName: String): Unit = {

        // 创建 sparkSession
        val sparkSession: SparkSession = SparkSessionBuilder.getOrCreateSparkSession(s"$tableName offline data clean")

        // 操作时间
        val operateTime = lit(current_timestamp())
        // 如果需要设置特定的时间，则使用下面的变量
        // val operateTime = lit(Timestamp.valueOf("2024-01-01 00:00:00"))

        // 读取 Hive 的 ods.odsTableName 表的最新分区数据
        val latestOdsPartition = sparkSession.sql(s"SELECT MAX(etl_date) FROM $odsTableName").collect()(0)(0).toString
        val odsData = sparkSession.sql(s"SELECT *, '$latestOdsPartition' as etl_date FROM $odsTableName WHERE etl_date='$latestOdsPartition'")

        // 添加新的列
        val transformedData = odsData
          .withColumn("dwd_insert_user", lit("user1"))
          .withColumn("dwd_insert_time", operateTime)
          .withColumn("dwd_modify_user", lit("user1"))
          .withColumn("dwd_modify_time", operateTime)

        // 数据写入到 hive dwd.dwdTableName
        transformedData.write.mode(SaveMode.Append).insertInto(dwdTableName)

        // 关闭 sparkSession
        sparkSession.stop()
    }
}

package com.kanseiu.spark.handler

import com.kanseiu.spark.common.SparkSessionBuilder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object CustomerInfDataClean {

    def main(args: Array[String]): Unit = {

        // 定义变量
        val tableName: String = "customer_inf"
        val odsTableName: String = "ods.customer_inf"
        val dwdTableName: String = "dwd.dim_customer_inf"
        val mergeCol: String = "customer_id"
        val orderByCol: String = "modified_time"

        // 创建 sparkSession
        val sparkSession: SparkSession = SparkSessionBuilder.getOrCreateSparkSession(s"$tableName offline data clean")

        // 查询ods和dwd层最新分区的日期
        val latestOdsPartition = sparkSession.sql(s"SELECT MAX(etl_date) FROM $odsTableName").collect()(0)(0).toString
        val latestDwdPartition = sparkSession.sql(s"SELECT MAX(etl_date) FROM $dwdTableName").collect()(0)(0).toString

        // 抽取 odsTableName 的最新分区的数据
        val odsData = sparkSession.sql(s"SELECT * FROM $odsTableName WHERE etl_date = '$latestOdsPartition'")
        // 抽取 dwdTableName 的最新分区的数据
        val dwdData = sparkSession.sql(s"SELECT * FROM $dwdTableName WHERE etl_date = '$latestDwdPartition'")

        // 为了区分同一条记录在ODS和DWD中的版本，给ODS和DWD数据添加来源标记
        val odsMarked = odsData.withColumn("data_source", lit("ods"))
        val dwdMarked = dwdData.withColumn("data_source", lit("dwd"))

        // 将ODS和DWD数据集合并
        // 根据字段名称合并odsData和dwdData的数据
        val combinedData = odsMarked.unionByName(dwdMarked)
          // 增加新的 current_timestamp 字段备用
          .withColumn("current_timestamp", current_timestamp())
          // 数据合并逻辑：按照 mergeCol 分组数据，相同 mergeCol 的数据在一组，之后根据 orderByCol 时间字段排序
          .withColumn("rank", row_number().over(Window.partitionBy(col(s"$mergeCol")).orderBy(col(s"$orderByCol").desc)))
          // 取组内最新一条的数据
          .where(col("rank") === 1)
          // 删除不再需要的 rank 字段
          .drop("rank")
          .selectExpr(
              "customer_id",
              "customer_name",
              "customer_level",
              "extend_info",
              "modified_time",

              "'user1' as dwd_insert_user",
              // 如果是DWD数据源，则直接使用原有的dwd_insert_time；如果是ODS数据源，则表示为新增或更新的数据
              "CASE WHEN data_source = 'ods' THEN current_timestamp ELSE dwd_insert_time END as dwd_insert_time",
              "'user1' as dwd_modify_user",
              // 如果是DWD数据源，则表示数据未变动，dwd_modify_time不变；如果是ODS数据源，则说明数据被更新，dwd_modify_time使用当前操作时间
              "CASE WHEN data_source = 'ods' THEN current_timestamp ELSE dwd_modify_time END as dwd_modify_time",

              s"'$latestOdsPartition' as etl_date"
          )

        // 显示合并后的数据，以便检查
        combinedData.show(false)

        // 将数据写入临时表
        combinedData.createOrReplaceTempView("temp_combined_data")

        // 使用Hive SQL将临时表数据写入到dwdTableName
        sparkSession.sql(
            s"""
                INSERT OVERWRITE TABLE $dwdTableName PARTITION (etl_date)
                SELECT
                    customer_id,
                    customer_name,
                    customer_level,
                    extend_info,
                    modified_time,

                    dwd_insert_user,
                    dwd_insert_time,
                    dwd_modify_user,
                    dwd_modify_time,
                    etl_date
                FROM
                    temp_combined_data
                """
        )

        // 关闭 sparksession
        sparkSession.stop()
    }
}
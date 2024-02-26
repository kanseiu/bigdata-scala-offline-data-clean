package com.kanseiu.spark.handler

import com.kanseiu.spark.common.SparkSessionBuilder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object CouponInfoDataClean {

    def main(args: Array[String]): Unit = {
        val tableName: String = "coupon_info"
        val odsTableName: String = "ods.coupon_info"
        val dwdTableName: String = "dwd.dim_coupon_info"
        val mergeCol: String = "coupon_id"
        val orderByCol: String = "modified_time"

        val sparkSession: SparkSession = SparkSessionBuilder.getOrCreateSparkSession(s"$tableName offline data clean")

        val latestOdsPartition = sparkSession.sql(s"SELECT MAX(etl_date) FROM $odsTableName").collect()(0)(0).toString
        val latestDwdPartition = sparkSession.sql(s"SELECT MAX(etl_date) FROM $dwdTableName").collect()(0)(0).toString

        val odsData = sparkSession.sql(s"SELECT * FROM $odsTableName WHERE etl_date = '$latestOdsPartition'")
        val dwdData = sparkSession.sql(s"SELECT * FROM $dwdTableName WHERE etl_date = '$latestDwdPartition'")

        val odsMarked = odsData.withColumn("data_source", lit("ods"))
        val dwdMarked = dwdData.withColumn("data_source", lit("dwd"))

        // 将ODS和DWD数据集合并，准备进行合并操作
        val combinedData = odsMarked.unionByName(dwdMarked)
          .withColumn("current_timestamp", current_timestamp())
          .withColumn("rank", row_number().over(Window.partitionBy(col(s"$mergeCol")).orderBy(col(s"$orderByCol").desc)))
          .where(col("rank") === 1)
          .drop("rank")
          .selectExpr(
              "coupon_id",
              "coupon_name",
              "coupon_type",
              "condition_amount",
              "condition_num",
              "activity_id",
              "benefit_amount",
              "benefit_discount",
              "modified_time",

              "'user1' as dwd_insert_user",
              "CASE WHEN data_source = 'ods' THEN current_timestamp ELSE dwd_insert_time END as dwd_insert_time",
              "'user1' as dwd_modify_user",
              "CASE WHEN data_source = 'ods' THEN current_timestamp ELSE dwd_modify_time END as dwd_modify_time",

              s"'$latestOdsPartition' as etl_date"
          )

        combinedData.show(false)

        combinedData.createOrReplaceTempView("temp_combined_data")

        sparkSession.sql(
        s"""
                INSERT OVERWRITE TABLE $dwdTableName PARTITION (etl_date)
                SELECT
                    coupon_id,
                    coupon_name,
                    coupon_type,
                    condition_amount,
                    condition_num,
                    activity_id,
                    benefit_amount,
                    benefit_discount,
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
        sparkSession.stop()
    }
}
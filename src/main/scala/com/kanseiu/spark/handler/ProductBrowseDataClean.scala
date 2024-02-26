package com.kanseiu.spark.handler

import com.kanseiu.spark.common.SparkSessionBuilder
import org.apache.hadoop.hbase.{CompareOperator, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.{RegexStringComparator, RowFilter}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{coalesce, current_timestamp, lit}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.util.Base64

object ProductBrowseDataClean {

    def main(args: Array[String]): Unit = {
        val tableName: String = "product_browse"
        val odsTableName: String = "ods.product_browse"
        val dwdTableName: String = "dwd.log_product_browse"
        val hbaseTableName: String = "ods:product_browse_offline"
        val mergeCol: String = "log_id"
        val extractDateRegex: String = ".*20221001.*"
        val currentTime = lit(current_timestamp())
        // val currentTime = lit(Timestamp.valueOf("2024-01-01 00:00:00"))

        val sparkSession: SparkSession = SparkSessionBuilder.getOrCreateSparkSession(s"$tableName offline data clean")

        import sparkSession.implicits._

        val hiveData = sparkSession.sql(
            s"""
            SELECT
                log_id AS hive_log_id,
                product_id AS hive_product_id,
                customer_id AS hive_customer_id,
                gen_order AS hive_gen_order,
                order_sn AS hive_order_sn,
                modified_time AS hive_modified_time,
                etl_date
            FROM
                $odsTableName
            WHERE
                etl_date = (SELECT max(etl_date) FROM $odsTableName)
            """
        )

        val sc: SparkContext = sparkSession.sparkContext
        val scan = new Scan()
        val comparator = new RegexStringComparator(extractDateRegex)
        val rowFilter = new RowFilter(CompareOperator.EQUAL, comparator)
        scan.setFilter(rowFilter)
        val proto = ProtobufUtil.toScan(scan)
        val scanStr = Base64.getEncoder.encodeToString(proto.toByteArray)
        val conf = HBaseConfiguration.create()
        conf.set(TableInputFormat.INPUT_TABLE, hbaseTableName)
        conf.set(TableInputFormat.SCAN, scanStr)
        val hbaseContext = new HBaseContext(sc, conf)
        val hbaseRDD = hbaseContext.hbaseRDD(TableName.valueOf(hbaseTableName), scan)

        val hbaseDF = hbaseRDD.map { case (_, result) =>
            val log_id = Bytes.toInt(result.getValue(Bytes.toBytes("Info"), Bytes.toBytes("log_id")))
            val product_id = Bytes.toInt(result.getValue(Bytes.toBytes("Info"), Bytes.toBytes("product_id")))
            val customer_id = Bytes.toInt(result.getValue(Bytes.toBytes("Info"), Bytes.toBytes("customer_id")))
            val gen_order = Bytes.toInt(result.getValue(Bytes.toBytes("Info"), Bytes.toBytes("gen_order")))
            val order_sn = Bytes.toString(result.getValue(Bytes.toBytes("Info"), Bytes.toBytes("order_sn")))
            val modified_time = Bytes.toString(result.getValue(Bytes.toBytes("Info"), Bytes.toBytes("modified_time")))

            Row(log_id, product_id, customer_id, gen_order, order_sn, modified_time)
        }

        val schema = StructType(Array(
            StructField("log_id", IntegerType),
            StructField("product_id", IntegerType),
            StructField("customer_id", IntegerType),
            StructField("gen_order", IntegerType),
            StructField("order_sn", StringType),
            StructField("modified_time", StringType)
        ))

        val hbaseData = sparkSession.createDataFrame(hbaseDF, schema)

        val mergedData = hiveData.join(hbaseData, Seq(mergeCol), "full_outer").select(
            coalesce($"hive_log_id", $"log_id").alias("log_id"),
            coalesce($"hive_product_id", $"product_id").alias("product_id"),
            coalesce($"hive_customer_id", $"customer_id").alias("customer_id"),
            coalesce($"hive_gen_order", $"gen_order").alias("gen_order"),
            coalesce($"hive_order_sn", $"order_sn").alias("order_sn"),
            coalesce($"hive_modified_time", $"modified_time").alias("modified_time"),

            lit("user1").alias("dwd_insert_user"),
            currentTime.alias("dwd_insert_time"),
            lit("user1").alias("dwd_modify_user"),
            currentTime.alias("dwd_modify_time"),
            $"etl_date"
        )

        mergedData.write.mode(SaveMode.Append).insertInto(dwdTableName)

        sparkSession.stop()
    }
}
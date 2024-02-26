package com.kanseiu.spark.handler

import com.kanseiu.spark.common.SparkSessionBuilder
import org.apache.hadoop.hbase.{CompareOperator, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.{RegexStringComparator, RowFilter}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{coalesce, current_timestamp, lit}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

import java.util.Base64

object OrderDetailDataClean {

    def main(args: Array[String]): Unit = {
        val tableName: String = "order_detail"
        val odsTableName: String = "ods.order_detail"
        val dwdTableName: String = "dwd.fact_order_detail"
        val hbaseTableName: String = "ods:order_detail_offline"
        val mergeCol: String = "order_detail_id"
        val extractDateRegex: String = ".*20221001.*"
        val currentTime = lit(current_timestamp())
        // val currentTime = lit(Timestamp.valueOf("2024-01-01 00:00:00"))

        val sparkSession: SparkSession = SparkSessionBuilder.getOrCreateSparkSession(s"$tableName offline data clean")

        import sparkSession.implicits._

        val hiveData = sparkSession.sql(s"""
            SELECT
                order_detail_id AS hive_order_detail_id,
                order_sn AS hive_order_sn,
                product_id AS hive_product_id,
                product_name AS hive_product_name,
                product_cnt AS hive_product_cnt,
                product_price AS hive_product_price,
                average_cost AS hive_average_cost,
                weight AS hive_weight,
                fee_money AS hive_fee_money,
                w_id AS hive_w_id,
                create_time AS hive_create_time,
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
            val order_detail_id = Bytes.toInt(result.getValue(Bytes.toBytes("Info"), Bytes.toBytes("order_detail_id")))
            val order_sn = Bytes.toString(result.getValue(Bytes.toBytes("Info"), Bytes.toBytes("order_sn")))
            val product_id = Bytes.toInt(result.getValue(Bytes.toBytes("Info"), Bytes.toBytes("product_id")))
            val product_name = Bytes.toString(result.getValue(Bytes.toBytes("Info"), Bytes.toBytes("product_name")))
            val product_cnt = Bytes.toInt(result.getValue(Bytes.toBytes("Info"), Bytes.toBytes("product_cnt")))
            val product_price = Bytes.toDouble(result.getValue(Bytes.toBytes("Info"), Bytes.toBytes("product_price")))
            val average_cost = Bytes.toDouble(result.getValue(Bytes.toBytes("Info"), Bytes.toBytes("average_cost")))
            val weight = Bytes.toDouble(result.getValue(Bytes.toBytes("Info"), Bytes.toBytes("weight")))
            val fee_money = Bytes.toDouble(result.getValue(Bytes.toBytes("Info"), Bytes.toBytes("fee_money")))
            val w_id = Bytes.toInt(result.getValue(Bytes.toBytes("Info"), Bytes.toBytes("w_id")))
            val create_time = Bytes.toString(result.getValue(Bytes.toBytes("Info"), Bytes.toBytes("create_time")))
            val modified_time = Bytes.toString(result.getValue(Bytes.toBytes("Info"), Bytes.toBytes("modified_time")))

            Row(order_detail_id, order_sn, product_id, product_name, product_cnt, product_price, average_cost,
                weight, fee_money, w_id, create_time, modified_time)
        }

        val schema = StructType(Array(
            StructField("order_detail_id", IntegerType),
            StructField("order_sn", StringType),
            StructField("product_id", IntegerType),
            StructField("product_name", StringType),
            StructField("product_cnt", IntegerType),
            StructField("product_price", DoubleType),
            StructField("average_cost", DoubleType),
            StructField("weight", DoubleType),
            StructField("fee_money", DoubleType),
            StructField("w_id", IntegerType),
            StructField("create_time", StringType),
            StructField("modified_time", StringType)
        ))

        val hbaseData = sparkSession.createDataFrame(hbaseDF, schema)

        val mergedData = hiveData.join(hbaseData, Seq(mergeCol), "full_outer").select(
            coalesce($"hive_order_detail_id", $"order_detail_id").alias("order_detail_id"),
            coalesce($"hive_order_sn", $"order_sn").alias("order_sn"),
            coalesce($"hive_product_id", $"product_id").alias("product_id"),
            coalesce($"hive_product_name", $"product_name").alias("product_name"),
            coalesce($"hive_product_cnt", $"product_cnt").alias("product_cnt"),
            coalesce($"hive_product_price", $"product_price").alias("product_price"),
            coalesce($"hive_average_cost", $"average_cost").alias("average_cost"),
            coalesce($"hive_weight", $"weight").alias("weight"),
            coalesce($"hive_fee_money", $"fee_money").alias("fee_money"),
            coalesce($"hive_w_id", $"w_id").alias("w_id"),
            coalesce($"hive_create_time", $"create_time").alias("create_time"),
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
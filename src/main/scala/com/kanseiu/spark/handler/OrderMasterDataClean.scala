package com.kanseiu.spark.handler

import com.kanseiu.spark.common.Constants._
import com.kanseiu.spark.common.HbaseCliUtil._
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

object OrderMasterDataClean {

    def main(args: Array[String]): Unit = {
        // 定义变量
        val tableName: String = "order_master"
        val odsTableName: String = "ods.order_master"
        val dwdTableName: String = "dwd.fact_order_master"
        val hbaseTableName: String = "ods:order_master_offline"
        val mergeCol: String = "order_id"
        // 定义正则表达式匹配2022年10月01日的数据，如果变成别的日期则修改
        val extractDateRegex: String = ".*20221001.*"
        // 定义 insertTime 和 modifiedTime 为当前时间
        val currentTime = lit(current_timestamp())
        // 如果需要使用指定的时间戳，比如2024-01-01 00:00:00
        // val currentTime = lit(Timestamp.valueOf("2024-01-01 00:00:00"))

        // 创建 sparkSession
        val sparkSession: SparkSession = SparkSessionBuilder.getOrCreateSparkSession(s"$tableName offline data clean")

        import sparkSession.implicits._

        // 从 hive ods.order_master 中读取最新分区数据
        val hiveData = sparkSession.sql(s"""
            SELECT
                order_id AS hive_order_id,
                order_sn AS hive_order_sn,
                customer_id AS hive_customer_id,
                shipping_user AS hive_shipping_user,
                province AS hive_province,
                city AS hive_city,
                address AS hive_address,
                order_source AS hive_order_source,
                payment_method AS hive_payment_method,
                order_money AS hive_order_money,
                district_money AS hive_district_money,
                shipping_money AS hive_shipping_money,
                payment_money AS hive_payment_money,
                shipping_comp_name AS hive_shipping_comp_name,
                shipping_sn AS hive_shipping_sn,
                create_time AS hive_create_time,
                shipping_time AS hive_shipping_time,
                pay_time AS hive_pay_time,
                receive_time AS hive_receive_time,
                order_status AS hive_order_status,
                order_point AS hive_order_point,
                invoice_title AS hive_invoice_title,
                modified_time AS hive_modified_time,
                etl_date
            FROM
                $odsTableName
            WHERE
                etl_date = (SELECT max(etl_date) FROM $odsTableName)
            """
        )

        // 从 hbase 中读取指定的数据
        val sc: SparkContext = sparkSession.sparkContext
        val scan = new Scan()

        // 使用正则表达式过滤器来匹配特定日期的数据
        val comparator = new RegexStringComparator(extractDateRegex)
        val rowFilter = new RowFilter(CompareOperator.EQUAL, comparator)
        scan.setFilter(rowFilter)

        // 将Scan对象转换为协议缓冲区格式，并进行Base64编码
        val proto = ProtobufUtil.toScan(scan)
        val scanStr = Base64.getEncoder.encodeToString(proto.toByteArray)

        // 设置HBase配置
        val conf = HBaseConfiguration.create()
        conf.set(TableInputFormat.INPUT_TABLE, hbaseTableName)
        conf.set(TableInputFormat.SCAN, scanStr)

        // 使用HBaseContext读取HBase数据
        val hbaseContext = new HBaseContext(sc, conf)
        val hbaseRDD = hbaseContext.hbaseRDD(TableName.valueOf(hbaseTableName), scan)

        // 转换HBase数据为DataFrame所需的Row
        val hbaseDF = hbaseRDD.map{ case(_, result) =>
            Row(
                getValue[Int](result, colFamilyName, "order_id", Bytes.toInt),
                getValue[String](result, colFamilyName, "order_sn", Bytes.toString),
                getValue[Int](result, colFamilyName, "customer_id", Bytes.toInt),
                getValue[String](result, colFamilyName, "shipping_user", Bytes.toString),
                getValue[String](result, colFamilyName, "province", Bytes.toString),
                getValue[String](result, colFamilyName, "city", Bytes.toString),
                getValue[String](result, colFamilyName, "address", Bytes.toString),
                getValue[Int](result, colFamilyName, "order_source", Bytes.toInt),
                getValue[Int](result, colFamilyName, "payment_method", Bytes.toInt),
                getValue[Double](result, colFamilyName, "order_money", Bytes.toDouble),
                getValue[Double](result, colFamilyName, "district_money", Bytes.toDouble),
                getValue[Double](result, colFamilyName, "shipping_money", Bytes.toDouble),
                getValue[Double](result, colFamilyName, "payment_money", Bytes.toDouble),
                getValue[String](result, colFamilyName, "shipping_comp_name", Bytes.toString),
                getValue[String](result, colFamilyName, "shipping_sn", Bytes.toString),
                getValue[String](result, colFamilyName, "create_time", Bytes.toString),
                getValue[String](result, colFamilyName, "shipping_time", Bytes.toString),
                getValue[String](result, colFamilyName, "pay_time", Bytes.toString),
                getValue[String](result, colFamilyName, "receive_time", Bytes.toString),
                getValue[String](result, colFamilyName, "order_status", Bytes.toString),
                getValue[Int](result, colFamilyName, "order_point", Bytes.toInt),
                getValue[String](result, colFamilyName, "invoice_title", Bytes.toString),
                getValue[String](result, colFamilyName, "modified_time", Bytes.toString)
            )
        }

        // 定义DataFrame的schema
        val schema = StructType(Array(
            StructField("order_id", IntegerType),
            StructField("order_sn", StringType),
            StructField("customer_id", IntegerType),
            StructField("shipping_user", StringType),
            StructField("province", StringType),
            StructField("city", StringType),
            StructField("address", StringType),
            StructField("order_source", IntegerType),
            StructField("payment_method", IntegerType),
            StructField("order_money", DoubleType),
            StructField("district_money", DoubleType),
            StructField("shipping_money", DoubleType),
            StructField("payment_money", DoubleType),
            StructField("shipping_comp_name", StringType),
            StructField("shipping_sn", StringType),
            StructField("create_time", StringType),
            StructField("shipping_time", StringType),
            StructField("pay_time", StringType),
            StructField("receive_time", StringType),
            StructField("order_status", StringType),
            StructField("order_point", IntegerType),
            StructField("invoice_title", StringType),
            StructField("modified_time", StringType)
        ))

        // 使用提取的数据和定义的schema创建DataFrame
        val hbaseData = sparkSession.createDataFrame(hbaseDF, schema)

        // 合并hive 和 hbase 中的数据
        val mergedData = hiveData.join(hbaseData, Seq(mergeCol), "full_outer").select(
            coalesce($"hive_order_id", $"order_id").alias("order_id"),
            coalesce($"hive_order_sn", $"order_sn").alias("order_sn"),
            coalesce($"hive_customer_id", $"customer_id").alias("customer_id"),
            coalesce($"hive_shipping_user", $"shipping_user").alias("shipping_user"),
            coalesce($"hive_province", $"province").alias("province"),
            coalesce($"hive_city", $"city").alias("city"),
            coalesce($"hive_address", $"address").alias("address"),
            coalesce($"hive_order_source", $"order_source").alias("order_source"),
            coalesce($"hive_payment_method", $"payment_method").alias("payment_method"),
            coalesce($"hive_order_money", $"order_money").alias("order_money"),
            coalesce($"hive_district_money", $"district_money").alias("district_money"),
            coalesce($"hive_shipping_money", $"shipping_money").alias("shipping_money"),
            coalesce($"hive_payment_money", $"payment_money").alias("payment_money"),
            coalesce($"hive_shipping_comp_name", $"shipping_comp_name").alias("shipping_comp_name"),
            coalesce($"hive_shipping_sn", $"shipping_sn").alias("shipping_sn"),
            coalesce($"hive_create_time", $"create_time").alias("create_time"),
            coalesce($"hive_shipping_time", $"shipping_time").alias("shipping_time"),
            coalesce($"hive_pay_time", $"pay_time").alias("pay_time"),
            coalesce($"hive_receive_time", $"receive_time").alias("receive_time"),
            coalesce($"hive_order_status", $"order_status").alias("order_status"),
            coalesce($"hive_order_point", $"order_point").alias("order_point"),
            coalesce($"hive_invoice_title", $"invoice_title").alias("invoice_title"),
            coalesce($"hive_modified_time", $"modified_time").alias("modified_time"),

            lit("user1").alias("dwd_insert_user"),
            currentTime.alias("dwd_insert_time"),
            lit("user1").alias("dwd_modify_user"),
            currentTime.alias("dwd_modify_time"),
            $"etl_date"
        )

        // 使用追加模式，写入数据到 dwd，不考虑和dwd数据的合并问题
        mergedData.write.mode(SaveMode.Append).insertInto(dwdTableName)

        sparkSession.stop()
    }
}
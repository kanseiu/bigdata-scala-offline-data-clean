package com.kanseiu.spark.handler

import com.kanseiu.spark.common.Constants
import org.apache.hadoop.hbase.{CompareOperator, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.{RegexStringComparator, RowFilter}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import java.util.Base64

object OrderDetailDataClean {

    def main(args: Array[String]): Unit = {
        // 定义变量
        val tableName: String = "order_detail"
        val odsTableName: String = "ods.order_detail"
        val dwdTableName: String = "dwd.fact_order_detail"
        val hbaseTableName: String = "ods:order_detail_offline"
        val mergeCol: String = "order_detail_id"
        // 定义正则表达式匹配2022年10月01日的数据，如果变成别的日期则修改
        val extractDateRegex: String = ".*20221001.*"

        // 创建 sparkSession
        val sparkSession: SparkSession = SparkSession.builder
          .appName(s"$tableName offline data clean")
          .config("spark.sql.warehouse.dir", Constants.sparkWarehouse)
          .config("hive.metastore.uris", Constants.metastoreUris)
          .config("spark.executor.memory", "512m") // 根据需要设置 executor 内存
          .config("hive.exec.dynamic.partition.mode", "nonstrict")
          .enableHiveSupport()
          .getOrCreate()

        // 从 hive ods.order_detail 中读取最新分区数据
        val hiveData = sparkSession.sql(s"""
            SELECT
                *,
                'user1' as dwd_insert_user,
                current_timestamp() as dwd_insert_time,
                'user1' as dwd_modify_user,
                current_timestamp() as dwd_modify_time
            FROM
                $odsTableName
            WHERE
                etl_date = (SELECT max(etl_date) FROM $odsTableName)
            """
        )

        // 配置 HBaseContext 并从hbase中读取数据
        val sc: SparkContext = sparkSession.sparkContext
        val scan = new Scan()
        val comparator = new RegexStringComparator(extractDateRegex)
        val rowFilter = new RowFilter(CompareOperator.EQUAL, comparator)
        scan.setFilter(rowFilter)
        val proto = ProtobufUtil.toScan(scan)
        val scanStr = Base64.getEncoder.encodeToString(proto.toByteArray)

        val conf = HBaseConfiguration.create()
        val hbaseContext = new HBaseContext(sc, conf)
        conf.set(TableInputFormat.INPUT_TABLE, hbaseTableName)
        conf.set(TableInputFormat.SCAN, scanStr)
        val hbaseRDD = hbaseContext.hbaseRDD(TableName.valueOf(hbaseTableName), scan)

        // 读取hbase数据，并转换成row
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

        val df = sparkSession.createDataFrame(hbaseDF, schema)

        // 合并数据
        val mergedDF = hiveData.join(df, Seq(mergeCol), "left_outer")
        mergedDF.write.mode(SaveMode.Overwrite).insertInto(dwdTableName)

        sparkSession.stop()
    }
}
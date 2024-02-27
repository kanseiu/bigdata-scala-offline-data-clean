package com.kanseiu.spark.handler

import com.kanseiu.spark.common.SimpleDataCleanUtils

object OrderCartDataClean {

    def main(args: Array[String]): Unit = {
        val tableName: String = "order_cart"
        val odsTableName: String = "ods.order_cart"
        val dwdTableName: String = "dwd.fact_order_cart"

        SimpleDataCleanUtils.processTable(tableName, odsTableName, dwdTableName)
    }
}
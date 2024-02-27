package com.kanseiu.spark.handler

import com.kanseiu.spark.common.SimpleDataCleanUtils

object CustomerAddrDataClean {

    def main(args: Array[String]): Unit = {
        val tableName: String = "customer_addr"
        val odsTableName: String = "ods.customer_addr"
        val dwdTableName: String = "dwd.dim_customer_addr"

        SimpleDataCleanUtils.processTable(tableName, odsTableName, dwdTableName)
    }
}
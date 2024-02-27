package com.kanseiu.spark.handler

import com.kanseiu.spark.common.SimpleDataCleanUtils

object CustomerLevelInfDataClean {

    def main(args: Array[String]): Unit = {
        val tableName: String = "customer_level_inf"
        val odsTableName: String = "ods.customer_level_inf"
        val dwdTableName: String = "dwd.dim_customer_level_inf"

        SimpleDataCleanUtils.processTable(tableName, odsTableName, dwdTableName)
    }
}
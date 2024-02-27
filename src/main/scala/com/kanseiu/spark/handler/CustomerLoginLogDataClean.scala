package com.kanseiu.spark.handler

import com.kanseiu.spark.common.SimpleDataCleanUtils

object CustomerLoginLogDataClean {

    def main(args: Array[String]): Unit = {
        val tableName: String = "customer_login_log"
        val odsTableName: String = "ods.customer_login_log"
        val dwdTableName: String = "dwd.log_customer_login"

        SimpleDataCleanUtils.processTable(tableName, odsTableName, dwdTableName)
    }
}
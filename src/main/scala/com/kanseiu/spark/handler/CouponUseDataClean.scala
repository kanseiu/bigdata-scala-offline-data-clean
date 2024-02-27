package com.kanseiu.spark.handler

import com.kanseiu.spark.common.SimpleDataCleanUtils

object CouponUseDataClean {

    def main(args: Array[String]): Unit = {
        // 变量定义
        val tableName: String = "coupon_use"
        val odsTableName: String = "ods.coupon_use"
        val dwdTableName: String = "dwd.fact_coupon_use"

        // 数据清洗
        SimpleDataCleanUtils.processTable(tableName, odsTableName, dwdTableName)
    }
}
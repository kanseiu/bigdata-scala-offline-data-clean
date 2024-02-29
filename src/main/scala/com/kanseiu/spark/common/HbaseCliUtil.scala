package com.kanseiu.spark.common

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

object HbaseCliUtil {

    def getValue[T](result: Result, colFamilyName: String, columnName: String, converter: (Array[Byte] => T)): T = {
        val value = result.getValue(Bytes.toBytes(colFamilyName), Bytes.toBytes(columnName))
        converter(value)
    }

}
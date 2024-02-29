package com.kanseiu.spark.common

object Constants {

    // host
    private val host: String = "master"

    // sparkWarehouse
    val sparkWarehouse: String = s"hdfs://$host:9000/user/hive/warehouse"

    // metastoreUris
    val metastoreUris: String = s"thrift://$host:9083"

    val colFamilyName: String = "Info"

}

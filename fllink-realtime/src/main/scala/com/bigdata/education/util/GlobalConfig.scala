package com.bigdata.education.util

/**
 * Ciel
 * project-flink: com.bigdata.education.util
 * 2020-08-25 10:29:41
 */
object GlobalConfig {
    val HBASE_ZOOKEEPER_QUORUM = "hadoop102,hadoop103,hadoop104"
    val HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181"

    val BOOTSTRAP_SERVERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    val ACKS = "-1"
}

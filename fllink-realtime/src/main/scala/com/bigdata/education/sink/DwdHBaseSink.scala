package com.bigdata.education.sink

import com.bigdata.education.model.TopicAndValue
import com.bigdata.education.util.GlobalConfig
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

/**
 * Ciel
 * project-flink: com.bigdata.education.sink
 * 2020-08-25 10:33:20
 */
class DwdHBaseSink extends RichSinkFunction[TopicAndValue]{

    var connection:Connection = _

    override def open(parameters: Configuration): Unit = {
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum",GlobalConfig.HBASE_ZOOKEEPER_QUORUM)
        conf.set("hbase.zookeeper.property.clientport",GlobalConfig.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)
        connection = ConnectionFactory.createConnection(conf)
    }

    // 写入HBase
    override def invoke(value: TopicAndValue, context: SinkFunction.Context[_]): Unit = {
        value.topic match{
            case "basewebsite" => invokeBaseWebSite("education:dwd_basewebsite",value)
            case "based" => invokeBasedAd("education:dwd_based",value)
            case _ => invokeBaseVipLevel("education:dwd_membervip",value)
        }
    }

    override def close(): Unit = {
        super.close()
        connection.close()
    }

    def invokeBaseWebSite(tableName: String, value: TopicAndValue): Unit = ???

    def invokeBasedAd(tableName: String, value: TopicAndValue): Unit = ???

    def invokeBaseVipLevel(tableName: String, value: TopicAndValue): Unit = ???
}

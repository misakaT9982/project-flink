package com.bigdata.education.model

import com.google.gson.Gson
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Ciel
 * project-flink: com.bigdata.education.model
 * 2020-08-25 09:17:13
 */
class TopicAndValueDeserializationSchema extends KafkaDeserializationSchema[TopicAndValue]{
    override def isEndOfStream(nextElement: TopicAndValue): Boolean = false

    override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): TopicAndValue = {
        val gson = new Gson()
        gson.fromJson(new String(record.value(),"utf-8"),classOf[TopicAndValue])
    }

    override def getProducedType: TypeInformation[TopicAndValue] = {
        TypeInformation.of(new TypeHint[TopicAndValue] {})
    }
}

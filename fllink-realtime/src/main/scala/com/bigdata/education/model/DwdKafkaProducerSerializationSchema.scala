package com.bigdata.education.model

import java.nio.charset.Charset
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema
/**
 * Ciel
 * project-flink: com.bigdata.education.model
 * 2020-08-24 19:23:04
 */
class DwdKafkaProducerSerializationSchema extends KeyedSerializationSchema[TopicAndValue]{
    val serialVersionUID = 1351665280744549933L

    override def serializeKey(element: TopicAndValue): Array[Byte] = null

    override def serializeValue(element: TopicAndValue): Array[Byte] = {
        element.value.getBytes(Charset.forName("utf-8"))
    }

    override def getTargetTopic(element: TopicAndValue): String = {
        "dwd" + element.topic
    }
}

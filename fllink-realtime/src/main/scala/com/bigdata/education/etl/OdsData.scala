package com.bigdata.education.etl

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.bigdata.education.model.{DwdKafkaProducerSerializationSchema, TopicAndValue, TopicAndValueDeserializationSchema}
import com.bigdata.education.sink.DwdHBaseSink
import com.bigdata.education.util.{GlobalConfig, ParseJsonData}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.util.Collector

/**
 * Ciel
 * project-flink: com.bigdata.education.etl
 * 2020-08-25 09:49:08
 */
object OdsData extends App {
    val BOOTSTRAP_SERVERS = "bootstrap.servers"
    val GROUP_ID = "group.ip"
    val RETRIES = "retries"
    val TOPIC = "topic"

    val params: ParameterTool = ParameterTool.fromArgs(args)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.getGlobalJobParameters
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.enableCheckpointing(60000L) // 1分钟做一次checkPoint
    val checkpointConfig: CheckpointConfig = env.getCheckpointConfig
    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) // 仅仅一次
    checkpointConfig.setMinPauseBetweenCheckpoints(30000L) // 时间间隔为30分钟
    checkpointConfig.setCheckpointTimeout(10000L) // 设置超时时间
    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION) // cancel时保留CheckPoint

    // 设置stateBackend 为rocksDB
    val stateBackend = new RocksDBStateBackend("")
    env.setStateBackend(stateBackend)

    // 设置重启策略 重启3次 间隔10s
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)))

    import scala.collection.JavaConverters._

    val topicList = params.get(TOPIC).split(",").toBuffer.asJava
    val consumerPros = new Properties()
    consumerPros.getProperty(BOOTSTRAP_SERVERS, params.get(BOOTSTRAP_SERVERS))
    consumerPros.setProperty(GROUP_ID, params.get(GROUP_ID))
    val kafkaEventSource = new FlinkKafkaConsumer010[TopicAndValue](topicList, new TopicAndValueDeserializationSchema, consumerPros)
    kafkaEventSource.setStartFromEarliest()

    // 先过滤掉非json的数据
    val dataStream = env.addSource(kafkaEventSource).filter(item => {
        val jsonObject = ParseJsonData.getJsonData(item.value)
        jsonObject.isInstanceOf[JSONObject]
    })

    val sideOutHBaseTag = new OutputTag[TopicAndValue]("hbaseSinkStream")
    //val sideOutGreenPlumTag = new OutputTag[TopicAndValue]("greenplumSinkStream")
    val result = dataStream.process(new ProcessFunction[TopicAndValue, TopicAndValue] {
        override def processElement(value: TopicAndValue, ctx: ProcessFunction[TopicAndValue, TopicAndValue]#Context, out: Collector[TopicAndValue]): Unit = {
            value.topic match {
                case "basead" | "basewebside" | "membervip" => ctx.output(sideOutHBaseTag, value)
                case _ => out.collect(value)
            }
        }
    })

    // 得到侧输出流，写入HBase
    result.getSideOutput(sideOutHBaseTag).addSink(new DwdHBaseSink)

    result.addSink(new FlinkKafkaProducer010[TopicAndValue](GlobalConfig.BOOTSTRAP_SERVERS,"",new DwdKafkaProducerSerializationSchema))
    env.execute()
}

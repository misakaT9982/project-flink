package com.bigdata.flink.sql.api

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

/**
 * Ciel
 * project-flink: com.bigdata.flink.sql.api
 * 2020-07-07 20:17:55
 */
object KafkaPipeline extends App {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    // 输入表
    tableEnv.connect(
        new Kafka()
          .version("0.11")
          .topic("sensor")
          .property("zookeeper.connect", "realtime:2181")
          .property("bootstrap.service", "realtime:9092")
    )
      .withFormat(new Csv)
      .withSchema(
          new Schema()
            .field("id", DataTypes.STRING())
            .field("timestamp", DataTypes.BIGINT())
            .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")

    val sensorTable: Table = tableEnv.from("kafkaInputTable")
    val resultTable = sensorTable
      .select('id, 'temperature)
      .filter('id === 'sensor_1)

    val aggResultTable = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'cnt)

    // 输出表
    tableEnv.connect(
        new Kafka()
          .version("0.11")
          .topic("kafkaSensor")
          .property("zookeeper.connect", "realtime:2181")
          .property("bootstrap.service", "realtime:9092")
    )
      .withFormat(new Csv())
      .withSchema(
          new Schema()
            .field("id", DataTypes.STRING())
            .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaOutputTable")
    resultTable.insertInto("kafkaOutputTable")

    env.execute()
}

package com.bigdata.flink.sql.wc

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._

/**
 * Ciel
 * project-flink: com.bigdata.flink.sql
 * 2020-07-07 12:57:33
 */
object FlinkWorldCount_02 extends App {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 1. 老版本流式处理
    /*val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val oldTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    // 2. blink版本流式处理
    val blinks = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val blinkTable = StreamTableEnvironment.create(env, blinks)
    // 3. blink的批处理
    val blinkBatchTable = TableEnvironment.create(blinks)*/

    // 4. TODO 读取文件系统
    val filePath = "D:\\worksapce\\bigdata\\project-flink\\flink-lessons\\src\\main\\resources\\sensor.txt"
    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new OldCsv)
      .withSchema( // 定义表结构
          new Schema()
            .field("id", DataTypes.STRING())
            .field("timestamp", DataTypes.BIGINT())
            .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable") // 注册结构表

    val inputTable = tableEnv.from("inputTable")
    //inputTable.toAppendStream[(String, Long, Double)].print()

    // 5. 读取kafka系统
    tableEnv.connect(
        new Kafka()
          .version("0.11")
          .topic("sensor")
          .property("zookeeper.connect", "raltime102:2181")
          .property("bootstrap.service", "realtime102:9092")
    )
      .withFormat(new Csv())
      .withSchema(
          new Schema()
            .field("id", DataTypes.STRING())
            .field("timestamp", DataTypes.BIGINT())
            .field("temperature", DataTypes.DOUBLE())
      ).createTemporaryTable("kafkaInputTable")

    // 6. 表的查询和转换
    val sensorTable = tableEnv.from("inputTable")
    // 6.1 简单查询
    val resultTable = sensorTable
      .select('id, 'temperature)
      .filter('id === "sensor_1")
    tableEnv.sqlQuery("select id, temperature from "  )
    // 6.2 聚合查询
    val aggResultTable = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'count)

    val aggResultSqlTable = tableEnv.sqlQuery("select id , count(id) as cnt from inputTable group by id")

    resultTable.toAppendStream[(String, Double)].print("result -> ")
    aggResultTable.toRetractStream[(String, Long)].print("agg -> ")
    aggResultSqlTable.toRetractStream[(String, Long)].print("sql -> ")

    val kafkaInputTable = tableEnv.from("kafkaInputTable")
    //kafkaInputTable.toAppendStream[(String, Long, Double)].print()
    env.execute()

}

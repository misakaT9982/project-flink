package com.bigdata.flink.sql.api

import com.bigdata.flink.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Over, Table, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * Ciel
 * project-flink: com.bigdata.flink.sql.api
 * 2020-07-08 09:59:31
 */
object TimeAndWindow extends App {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val fileStream = env.readTextFile("D:\\worksapce\\bigdata\\project-flink\\flink-lessons\\src\\main\\resources\\sensor.txt")
    val dataStream = fileStream.map(data => {
        val datas = data.split(",")
        SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
    })
      .assignTimestampsAndWatermarks(
          new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
          override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })
    // 处理时间直接proctime
    //val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp, 'pt.proctime)
    // rowtime追加事件时间
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)

    // 1. Group Window聚合操作
    val resultTable: Table = sensorTable
        .window(Tumble over 10.seconds on 'ts as 'tw)
        .groupBy('dt, 'tw)
        .select('id ,'id.count, 'tw.end)

    // 2. Over Window聚合操作
    val overResultTable: Table = sensorTable
        .window(Over partitionBy 'id orderBy 'ts preceding 2.rows as 'ow)
        .select('id, 'ts, 'id.count over 'ow, 'temperatrue.avg over 'ow )

    // 3. SQL 实现Group By
    tableEnv.createTemporaryView("sensor",sensorTable)
    val resultSqlTable = tableEnv.sqlQuery(
        """
          |select id, count(id), hop_end(ts,interval '4' second, interval '10' second)
          |from sensor
          |group by id, hop(ts,interval '4' second, interval '10' second)
          |""".stripMargin)

    // 4. Over Window
    val overSQLTable = tableEnv.sqlQuery(
        """
          |select id, count(id) over w, avg(temperature) over w
          |from sensor
          |window w as(
          |     partition by id
          |     order by ts
          |     rows between 2 preceding and current row
          |)
          |""".stripMargin)

    //resultTable.toRetractStream[Row].print("agg")
    //overResultTable.toAppendStream[Row].print("over")
    resultSqlTable.toAppendStream[Row].print("aggSQL")
    overSQLTable.toAppendStream[Row].print("overSQL")

    //sensorTable.printSchema()
    env.execute()

}

package com.bigdata.flink.sql.api

import com.bigdata.flink.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

/**
 * Ciel
 *
 * project-flink: com.bigdata.flink.sql.api
 * 2020-08-25 15:43:51
 */
object ScalarFunctions extends App {
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

    val hashCode: HashCode = new HashCode(1.0)

    // table API
    val resultTable = sensorTable.select('id, 'ts, hashCode('id))
    resultTable.toAppendStream[Row].print("api>>>")

    // SQL
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("hashCode", hashCode)
    val resultSQLTable = tableEnv.sqlQuery(
        """
          |select id, ts, hashCode(id)
          |from sensor
          |""".stripMargin)

    resultSQLTable.toAppendStream[Row].print("sql>>>")

    env.execute("")

    // 自定义求hash code的标量函数
    class HashCode(factor: Double) extends ScalarFunction {
        def eval(value: String): Int = {
            (value.hashCode * factor).toInt
        }
    }
}

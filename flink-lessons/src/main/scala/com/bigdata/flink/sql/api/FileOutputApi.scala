package com.bigdata.flink.sql.api

import com.bigdata.flink.bean.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
 * Ciel
 * project-flink: com.bigdata.flink.sql.api
 * 2020-07-07 15:32:14
 */
object FileOutputApi extends App {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val inputDStream: DataStream[String] = env.readTextFile("D:\\worksapce\\bigdata\\project-flink\\flink-lessons\\src\\main\\resources\\sensor.txt")
    val dataDStream: DataStream[SensorReading] = inputDStream.map(data => {
        val datas: Array[String] = data.split(",")
        SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
    })
    val sensorTable: Table = tableEnv.fromDataStream(dataDStream, 'id, 'temperature as 'temp, 'timestamp as 'ts)
    val resultTable = sensorTable
      .select('id, 'temp)
      .filter('id === "sensor_1")

    val aggResultTable = sensorTable
        .groupBy('id)
        .select('id,'id.count as 'cnt)

    tableEnv.connect(new FileSystem().path("D:\\worksapce\\bigdata\\project-flink\\flink-lessons\\src\\main\\resources\\sensorOut.txt"))
        .withFormat(new Csv())
        .withSchema(
            new Schema()
              .field("id",DataTypes.STRING())
              .field("temp",DataTypes.DOUBLE())
              //.field("cnt",DataTypes.BIGINT())
        )
        .createTemporaryTable("outputTable")
    resultTable.insertInto("outputTable")
    env.execute()

}

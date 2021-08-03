package com.bigdata.flink.sql.wc

import com.bigdata.flink.bean.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

/**
 * Ciel
 * project-flink: com.bigdata.flink.sql
 * 2020-07-07 11:51:24
 */
object FlinkWorldCount_01 {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        val fileData: DataStream[String] = env.readTextFile("D:\\worksapce\\bigdata\\project-flink\\flink-lessons\\src\\main\\resources\\sensor.txt")
        val dataStream: DataStream[SensorReading] = fileData.map(data => {
            val datas: Array[String] = data.split(",")
            SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
        })
        // 1. 基于table创建流环境
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
        // 2. 基于tableEnv将流转换成表，注册表
        val dataTable = tableEnv.fromDataStream(dataStream)
        // 3. 调用table的API，做转换表操作
        val resultTable:Table = dataTable
          .select("id,temperature")
          .filter("id == 'sensor_1'")
        // SQL实现
        tableEnv.registerTable("dataTable", dataTable)
        val resultSQLTable:Table = tableEnv.sqlQuery(
            """
              |select id, temperature
              |from dataTable
              |where id ='sensor_1'
              |""".stripMargin)

        // 拼接字符串会产生序列化，底层会默认构建一个view
        //tableEnv.sqlQuery("select id, temperature from " + dataTable + "where id = 'sensor_1")

        val resultDStrem: DataStream[(String, Double)] = resultTable
          .toAppendStream[(String, Double)]
        //resultDStrem.print()
        val resultsql = resultSQLTable
          .toAppendStream[(String, Double)]
        resultsql.print()
        env.execute("table api example job")
    }
}

package com.bigdata.flink.service

import java.util

import com.bigdata.flink.bean.OrderLogData
import com.bigdata.flink.dao.OrderTimeoutAnalysisDao
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Ciel
 * project-flink: com.bigdata.flink.service
 * 2020-07-03 12:10:42
 */
class OrderTimeoutAnalysisWithCEPService {
    private val orderTimeoutAnalysisDao = new OrderTimeoutAnalysisDao

    def getOrderTimeOutWithCEPDatas(source: String): DataStream[String] = {
        val fileDS = orderTimeoutAnalysisDao.readTextFile(source)
        val dataDS = fileDS.map(line => {
            val datas = line.split(",")
            OrderLogData(datas(0).toLong, datas(1), datas(2), datas(3).toLong)
        })
        val tsDS = dataDS.assignAscendingTimestamps(_.timestamp * 1000L)
        val orderKS = tsDS.keyBy(_.orderId)
        val p = Pattern.begin[OrderLogData]("begin")
          .where(_.status == "create")
          .followedBy("follow")
          .where(_.status == "pay")
          .within(Time.minutes(15))

        val ps = CEP.pattern(orderKS, p)

        val orderTimeOutTag = new OutputTag[String]("orderTimeOutTag")
        val resultDS = ps.select(
            orderTimeOutTag,
            new PatternTimeoutFunction[OrderLogData, String] {
                override def timeout(pattern: util.Map[String, util.List[OrderLogData]], timeoutTimestamp: Long): String = {
                    pattern.toString
                }
            },
            new PatternSelectFunction[OrderLogData, String] {
                override def select(pattern: util.Map[String, util.List[OrderLogData]]): String = {
                    pattern.toString
                }
            }
        )
        resultDS.getSideOutput(orderTimeOutTag).print("timeout")
        resultDS
    }

}

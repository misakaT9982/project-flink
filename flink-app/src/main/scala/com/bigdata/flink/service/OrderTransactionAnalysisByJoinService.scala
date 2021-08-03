package com.bigdata.flink.service

import com.bigdata.flink.bean.{OrderLogData, TXLogData}
import com.bigdata.flink.dao.OrderTransactionAnalysisDao
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * Ciel
 * project-flink: com.bigdata.flink.service
 * 2020-07-03 13:59:40
 */
class OrderTransactionAnalysisByJoinService {

    private val orderTransactionAnalysisDao = new OrderTransactionAnalysisDao

    def getOrderTransactionByJoinDatas(source1: String, source2: String): DataStream[(OrderLogData, TXLogData)] = {
        // TODO 1. 获取订单日志数据
        val fileDS1: DataStream[String] = orderTransactionAnalysisDao.readTextFile(source1)
        // TODO 2. 获取交易日志数据
        val fileDS2: DataStream[String] = orderTransactionAnalysisDao.readTextFile(source2)
        // TODO 3. 将日志数据转换成对应的样例类数据
        val orderDS = fileDS1.map(
            line => {
                val datas = line.split(",")
                OrderLogData(datas(0).toLong, datas(1), datas(2), datas(3).toLong)
            }
        )
        val payDS = orderDS
          .filter(_.txId != "")
          .assignAscendingTimestamps(_.timestamp * 1000L).keyBy(_.txId)

        val txDS = fileDS2.map(
            line => {
                val datas = line.split(",")
                TXLogData(datas(0), datas(1), datas(2).toLong)
            }
        )
        val txKS = txDS.assignAscendingTimestamps(_.timestamp * 1000L).keyBy(_.txId)
        payDS.intervalJoin(txKS)
          .between(Time.minutes(-5),Time.minutes(5))
          .process(
              new ProcessJoinFunction[OrderLogData,TXLogData,(OrderLogData,TXLogData)] {
                  override def processElement(left: OrderLogData, right: TXLogData, ctx: ProcessJoinFunction[OrderLogData, TXLogData, (OrderLogData, TXLogData)]#Context, out: Collector[(OrderLogData, TXLogData)]): Unit = {
                      out.collect((left,right))
                  }
              }
          )
    }
}

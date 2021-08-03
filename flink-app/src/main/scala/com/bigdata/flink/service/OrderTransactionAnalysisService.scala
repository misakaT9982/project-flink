package com.bigdata.flink.service

import com.bigdata.flink.bean.{OrderLogData, TXLogData}
import com.bigdata.flink.dao.OrderTransactionAnalysisDao
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * Ciel
 * project-flink: com.bigdata.flink.service
 * 2020-07-03 13:57:21
 */
class OrderTransactionAnalysisService {
    private val orderTransactionAnalysisDao = new OrderTransactionAnalysisDao


    def getOrderTransactionDatas(source1: String, source2: String): Unit = {
        val fileDS1: DataStream[String] = orderTransactionAnalysisDao.readTextFile(source1)
        val fileDS2 = orderTransactionAnalysisDao.readTextFile(source2)

        // TODO 3. 将日志数据转换成对应的样例类数据
        val orderDS = fileDS1.map(
            line => {
                val datas = line.split(",")
                OrderLogData(datas(0).toLong, datas(1), datas(2), datas(3).toLong)
            }
        )
        val orderKS = orderDS
          .assignAscendingTimestamps(_.timestamp * 1000L).keyBy(_.txId)

        val txDS = fileDS2.map(
            line => {
                val datas = line.split(",")
                TXLogData(datas(0), datas(1), datas(2).toLong)
            }
        )
        val txKS = txDS.assignAscendingTimestamps(_.timestamp * 1000L).keyBy(_.txId)
        val connectDS = orderKS.connect(txKS)
        val noOrderOutputTag = new OutputTag[String]("noOrderOutputTag")
        val notxOutputTag = new OutputTag[String]("notxOutputTag")
        connectDS.process(
            new CoProcessFunction[OrderLogData, TXLogData, String] {
                private var orderData: ValueState[OrderLogData] = _
                private var txData: ValueState[TXLogData] = _

                private var orderTimer: ValueState[Long] = _
                private var txTimer: ValueState[Long] = _

                override def open(parameters: Configuration): Unit = {
                    orderData = getRuntimeContext.getState(
                        new ValueStateDescriptor[OrderLogData]("orderData", classOf[OrderLogData])
                    )

                    txData = getRuntimeContext.getState(
                        new ValueStateDescriptor[TXLogData]("txData", classOf[TXLogData])
                    )

                    orderTimer = getRuntimeContext.getState(
                        new ValueStateDescriptor[Long]("orderTimer", classOf[Long])
                    )

                    txTimer = getRuntimeContext.getState(
                        new ValueStateDescriptor[Long]("txTimer", classOf[Long])
                    )
                }

                override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderLogData, TXLogData, String]#OnTimerContext, out: Collector[String]): Unit = {
                    if (orderData.value() != null) {
                        ctx.output(noOrderOutputTag, "交易[" + orderData.value.txId + "]无对账信息")
                    }
                    if (txData.value() != null) {
                        ctx.output(notxOutputTag, "交易[" + txData.value.txId + "]交易无支付信息")
                    }
                    orderData.clear()
                    txData.clear()
                    orderTimer.clear()
                    txTimer.clear()
                }

                override def processElement1(value: OrderLogData, ctx: CoProcessFunction[OrderLogData, TXLogData, String]#Context, out: Collector[String]): Unit ={

                }

                override def processElement2(value: TXLogData, ctx: CoProcessFunction[OrderLogData, TXLogData, String]#Context, out: Collector[String]): Unit = {

                }
            }
        )
    }


}

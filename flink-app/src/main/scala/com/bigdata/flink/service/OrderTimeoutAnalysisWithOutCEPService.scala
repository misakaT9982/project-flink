package com.bigdata.flink.service

import com.bigdata.flink.bean.OrderLogData
import com.bigdata.flink.dao.OrderTimeoutAnalysisDao
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * Ciel
 * project-flink: com.bigdata.flink.service
 * 2020-07-03 11:42:10
 */
class OrderTimeoutAnalysisWithOutCEPService {

    private val orderTimeoutAnalysisDao = new OrderTimeoutAnalysisDao

    def getOrderTimeOutWithOutCEPDatas(source: String): DataStream[String] = {
        val fileDS: DataStream[String] = orderTimeoutAnalysisDao.readTextFile(source)
        val dataDS = fileDS.map(line => {
            val datas = line.split(",")
            OrderLogData(datas(0).toLong, datas(1), datas(2), datas(3).toLong)
        })

        val tsDS = dataDS.assignAscendingTimestamps(_.timestamp * 1000L)
        val orderKS = tsDS.keyBy(_.orderId)

        val unmatchTag = new OutputTag[String]("unmatchTag")
        val processDS = orderKS.process(
            new KeyedProcessFunction[Long, OrderLogData, String] {
                private var payFlag: ValueState[Boolean] = _
                private var alarmTimer: ValueState[Long] = _

                override def open(parameters: Configuration): Unit = {
                    payFlag = getRuntimeContext
                      .getState(
                          new ValueStateDescriptor[Boolean]("payFlag", classOf[Boolean])
                      )
                    alarmTimer = getRuntimeContext
                      .getState(
                          new ValueStateDescriptor[Long]("alarmTimer", classOf[Long])
                      )
                }

                override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderLogData, String]#OnTimerContext, out: Collector[String]): Unit = {
                    if (payFlag.value()) {
                        ctx.output(unmatchTag, " ?????? " + ctx.getCurrentKey + " ?????????????????????????????? ?????? ??????????????? ")
                    } else {
                        ctx.output(unmatchTag, " ?????? " + ctx.getCurrentKey + " ?????????????????????????????? ?????? ??????????????? ")
                    }
                    payFlag.clear()
                    alarmTimer.clear()
                }

                override def processElement(value: OrderLogData, ctx: KeyedProcessFunction[Long, OrderLogData, String]#Context, out: Collector[String]): Unit = {
                    if (value.status == "create") {
                        // ?????????????????????
                        if (payFlag.value()) {
                            out.collect("??????" + ctx.getCurrentKey + "??????????????????")
                            ctx.timerService().deleteEventTimeTimer(alarmTimer.value())
                            payFlag.clear()
                            alarmTimer.clear()
                        } else {
                            // TODO ?????????????????????
                            // ??????????????????????????????????????????
                            val time = value.timestamp * 1000L + 15 * 60 * 1000L
                            ctx.timerService().registerEventTimeTimer (time)
                            alarmTimer.update(time)
                        }
                    } else if (value.status == "pay") {
                        // TODO ???????????????
                        if (alarmTimer.value() == 0L) {
                            val timerTime = value.timestamp * 1000L + 3 * 60 * 1000L
                            payFlag.update(true)
                            ctx.timerService().registerEventTimeTimer(timerTime)
                            alarmTimer.update(timerTime)
                        } else {
                            out.collect("?????????" + ctx.getCurrentKey + "??????????????????")
                            ctx.timerService() deleteEventTimeTimer (alarmTimer.value())
                            payFlag.clear()
                            alarmTimer.clear()
                        }
                    }

                }
            }
        )
        processDS.getSideOutput(unmatchTag).print("timeout")
        processDS
    }


}

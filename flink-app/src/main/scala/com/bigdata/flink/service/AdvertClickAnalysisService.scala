package com.bigdata.flink.service

import com.bigdata.flink.bean.{AdvClickCount, AdvClickData}
import com.bigdata.flink.dao.AdvertClickAnalysisDao
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * Ciel
 * project-flink: com.bigdata.flink.service
 * 2020-07-03 12:30:27
 */
class AdvertClickAnalysisService {
    private val advertClickAnalysisDao = new AdvertClickAnalysisDao

    def getAnalysisData(source: String): DataStream[AdvClickCount] = {
        val fileDS: DataStream[String] = advertClickAnalysisDao.readTextFile(source)
        val dataDs = fileDS.map(line => {
            val datas = line.split(",")
            AdvClickData(datas(0).toLong, datas(1).toLong, datas(2), datas(3), datas(4).toLong)
        })
        val tsDS = dataDs.assignAscendingTimestamps(_.timestamp)
        tsDS.keyBy(data => {
            data.userId + "_" + data.advId
        })
          .process(
              new KeyedProcessFunction[String, AdvClickData, AdvClickCount] {
                  private var countState: ValueState[Long] = _
                  private var sendErrorState: ValueState[Boolean] = _
                  private var resetTimer: ValueState[Long] = _

                  override def open(parameters: Configuration): Unit = {
                      countState = getRuntimeContext
                        .getState(
                            new ValueStateDescriptor[Long]("countState", classOf[Long])
                        )
                      sendErrorState = getRuntimeContext
                        .getState(
                            new ValueStateDescriptor[Boolean]("sendErrorState", classOf[Boolean])
                        )
                      resetTimer = getRuntimeContext
                        .getState(
                            new ValueStateDescriptor[Long]("resetTimer", classOf[Long])
                        )
                  }

                  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, AdvClickData, AdvClickCount]#OnTimerContext, out: Collector[AdvClickCount]): Unit = {
                      countState.clear()
                      sendErrorState.clear()
                      resetTimer.clear()
                  }

                  override def processElement(value: AdvClickData, ctx: KeyedProcessFunction[String, AdvClickData, AdvClickCount]#Context, out: Collector[AdvClickCount]): Unit = {

                      val currentCount: Long = countState.value()
                      if (currentCount == 0L) {
                          // 获取当前处理数据时间
                          // 2020-01-11 16:46:00
                          // 2020-01-12 00:00:00
                          // timestamp => Date => 日历类 + 1天 => timestamp
                          // timestamp => 天 + 1 => timestamp
                          if(resetTimer.value() == 0){
                              val ts = ctx.timerService().currentProcessingTime()
                              val day: Long = ts / (1000 * 60 * 60 * 24) + 1

                          }
                      }

                  }
              })
    }
}

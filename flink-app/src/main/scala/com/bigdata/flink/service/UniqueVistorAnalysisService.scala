package com.bigdata.flink.service

import com.bigdata.flink.bean.UserBehavior
import com.bigdata.flink.dao.UniqueVistorAnalysisDao
import com.bigdata.flink.functions.{UVByBloomFilterProcessFunction, UVProcessWindowFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow


/**
 * Ciel
 * project-flink: com.bigdata.flink.service
 * 2020-07-02 22:34:59
 */
class UniqueVistorAnalysisService {
    private val uniqueVistorAnalysisDao = new UniqueVistorAnalysisDao

    def getUV(source: String): DataStream[(String, Long)] = {
        val dataDS: DataStream[String] = uniqueVistorAnalysisDao.readTextFile(source)
        val uvDS = dataDS.map(line => {
            val datas = line.split(",")
            UserBehavior(datas(0).toLong, datas(1).toLong, datas(2).toInt, datas(3), datas(4).toLong)
        })
        val tsDS = uvDS.assignAscendingTimestamps(_.timestamp * 1000)

        tsDS
          .filter(_.behavior == "pv")
          .map(data => ("pv", data.userId))
          .keyBy(_._1)
          .timeWindow(Time.hours(1))
          .process(new UVProcessWindowFunction)
    }

    /**
     * 使用布隆过滤器
     *
     * @param source
     */
    def getUVByBloomFilter(source: String):DataStream[(String, Long)] = {
        val dataDS = uniqueVistorAnalysisDao.readTextFile(source)
        val uvDS = dataDS.map(line => {
            val datas = line.split(",")
            UserBehavior(datas(0).toLong, datas(1).toLong, datas(2).toInt, datas(3), datas(4).toLong)
        })
        val tsDS = uvDS.assignAscendingTimestamps(_.timestamp * 1000)
        tsDS
          .filter(_.behavior == "pv")
          .map(data => ("pv",data.userId))
          .keyBy(_._1)
          .timeWindow(Time.hours(1))
          .trigger(
              new Trigger[(String,Long),TimeWindow] {
                  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
                      TriggerResult.FIRE_AND_PURGE
                  }

                  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
                      TriggerResult.CONTINUE
                  }

                  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
                      TriggerResult.CONTINUE
                  }

                  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
              }
          )
          .process(new UVByBloomFilterProcessFunction)
    }
}

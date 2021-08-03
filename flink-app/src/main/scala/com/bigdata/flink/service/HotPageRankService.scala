package com.bigdata.flink.service

import java.text.SimpleDateFormat

import com.bigdata.flink.bean.ApacheLogEvent
import com.bigdata.flink.dao.HotPageRankDao
import com.bigdata.flink.functions.{HotPageRankAggregateFunction, HotPageRankKeyedProcessFunction, HotPageRankWindowFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * Ciel
 * project-flink: com.bigdata.flink.service
 * 2020-07-02 21:43:19
 */
class HotPageRankService {
    private val hotPageRankDao = new HotPageRankDao

    def getHotPageRank(rankNum: Int, source: String): DataStream[String] = {
        val dataDS = hotPageRankDao.readTextFile(source)
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val eventDS: DataStream[ApacheLogEvent] = dataDS.map(
            line => {
                val datas = line.split(" ")
                val ts = sdf.parse(datas(3)).getTime
                ApacheLogEvent(datas(0), datas(1), ts, datas(5), datas(6))
            })
        val etDS = eventDS.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.minutes(1)) {
                override def extractTimestamp(element: ApacheLogEvent): Long = {
                    element.eventTime
                }
            })
        val eventWS: WindowedStream[ApacheLogEvent, String, TimeWindow] = etDS
          .keyBy(_.url)
          .timeWindow(Time.minutes(10), Time.minutes(5))

        val aggWS = eventWS.aggregate(new HotPageRankAggregateFunction, new HotPageRankWindowFunction)
        aggWS.keyBy(_.windowEndTime)
          .process(new HotPageRankKeyedProcessFunction(rankNum))
    }
}

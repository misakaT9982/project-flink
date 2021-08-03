package com.bigdata.flink.service

import com.bigdata.flink.bean
import com.bigdata.flink.bean.UserBehavior
import com.bigdata.flink.dao.HotItemRankDao
import com.bigdata.flink.functions.{HotItemRankAggregateFunction, HotItemRankProcessFunction, HotItemWindowFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * Ciel
 * project-flink: com.bigdata.flink.service
 * 2020-07-02 18:44:38
 */
class HotItemRankService {

    private val hotItemRankDao = new HotItemRankDao

    def getHotItemRank(rankNum: Int, dataPath: String): DataStream[String] = {
        val dataDS: DataStream[String] = hotItemRankDao.readTextFile(dataPath)
        val ubDS = dataDS.map(line => {
            val datas = line.split(",")
            UserBehavior(datas(0).toLong, datas(1).toLong, datas(2).toInt, datas(3), datas(4).toLong)
        })
        val ubEventDS = ubDS.assignAscendingTimestamps(_.timestamp * 1000)
        val ubWS: WindowedStream[UserBehavior, Long, TimeWindow] = ubEventDS
          .filter(data => data.behavior == "pv")
          .keyBy(_.itemId)
          .timeWindow(Time.hours(1), Time.minutes(5))
        val iccDS: DataStream[bean.ItemClickCount] = ubWS.aggregate(new HotItemRankAggregateFunction, new HotItemWindowFunction)
        val iccKS = iccDS.keyBy(_.windowEndTime)
        iccKS.process(new HotItemRankProcessFunction(rankNum))
    }
}

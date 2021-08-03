package com.bigdata.flink.service

import com.bigdata.flink.bean.UserBehavior
import com.bigdata.flink.dao.PageViewAnalysisDao
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Ciel
 * project-flink: com.bigdata.flink.service
 * 2020-07-02 22:24:39
 */
class PageViewAnalysisService {
    private val pageViewAnalysisDao = new PageViewAnalysisDao

    def getPV(source: String): DataStream[(String, Int)] = {
        val dataDS = pageViewAnalysisDao.readTextFile(source)
        val ubDS = dataDS.map(line => {
            val datas = line.split(",")
            UserBehavior(datas(0).toLong, datas(1).toLong, datas(2).toInt, datas(3), datas(4).toLong)
        })
        val tsDS: DataStream[UserBehavior] = ubDS.assignAscendingTimestamps(_.timestamp * 1000)
        tsDS
          .filter(_.behavior == "pv")
          .map(data => ("pv", 1))
          .keyBy(_._1)
          .timeWindow(Time.hours(1))
          .sum(1)
    }
}

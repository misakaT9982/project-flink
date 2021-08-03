package com.bigdata.flink.service


import com.bigdata.flink.bean.{AppMarketingCount, AppMarketingData}
import com.bigdata.flink.dao.AppMarketingByChannelAnalysisDao
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * Ciel
 * project-flink: com.bigdata.flink.service
 * 2020-07-03 12:36:13
 */
class AppMarketingByChannelAnalysisService {
    private val appMarketingByChannelAnalysisDao = new AppMarketingByChannelAnalysisDao

    def getAnalysisDatas(): DataStream[AppMarketingCount] = {
        val dataDS = appMarketingByChannelAnalysisDao.mockData()
        val tsDS: DataStream[AppMarketingData] = dataDS.assignAscendingTimestamps(_.timestamp)
        val orderKS = tsDS.keyBy(data => {
            data.marketing + "_" + data.behavior
        })

        orderKS
          .timeWindow(Time.seconds(10))
          .process(
              function = new ProcessWindowFunction[AppMarketingData, AppMarketingCount, String, TimeWindow] {
                  override def process(key: String, context: Context, elements: Iterable[AppMarketingData], out: Collector[AppMarketingCount]): Unit = {
                      val keys = key.split("_")
                      out.collect(AppMarketingCount(keys(0), keys(1), elements.size))
                  }
              }
          )
    }
}

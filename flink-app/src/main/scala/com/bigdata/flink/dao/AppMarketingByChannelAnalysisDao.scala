package com.bigdata.flink.dao

import com.bigdata.flink.bean.AppMarketingData
import com.bigdata.flink.common.TDao
import com.bigdata.flink.util.FlinkStreamEnv
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

/**
 * Ciel
 * project-flink: com.bigdata.flink.dao
 * 2020-07-03 12:36:33
 */
class AppMarketingByChannelAnalysisDao extends TDao{

    def mockData(): DataStream[AppMarketingData] = {
        FlinkStreamEnv.get().addSource(
            new SourceFunction[AppMarketingData] {

                private var flg = true

                override def run(ctx: SourceFunction.SourceContext[AppMarketingData]): Unit = {

                    while (flg) {
                        ctx.collect(AppMarketingData("HUAWEI", "INSTALL", System.currentTimeMillis()))
                        Thread.sleep(100)
                    }
                }

                override def cancel(): Unit = {
                    flg = false
                }
            }
        )
    }

}

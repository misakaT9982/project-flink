package com.bigdata.flink.application

import com.bigdata.flink.common.TApplication
import com.bigdata.flink.controller.HotItemRankController

/**
 * Ciel
 * project-flink: com.bigdata.flink.application
 * 2020-07-02 18:39:47
 */
object HotItemRankApplication extends App with TApplication{

    start("file"){
        val hotItemRankController = new HotItemRankController
        hotItemRankController
          .getHotItemRank(3,"datasources\\UserBehavior.csv")
          .print()

    }close{

    }
}

package com.bigdata.flink.application

import com.bigdata.flink.common.TApplication
import com.bigdata.flink.controller.HotPageRankController

/**
 * Ciel
 * project-flink: com.bigdata.flink.application
 * 2020-07-02 21:41:56
 */
object HotPageRankApplication extends App with TApplication {
    start("fileText") {
        val hotPageRankController = new HotPageRankController
        hotPageRankController
          .getHotPageRank(3,"datasources\\apache.log")
          .print
    } close {

    }
}

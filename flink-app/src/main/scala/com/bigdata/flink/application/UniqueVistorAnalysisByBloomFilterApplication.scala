package com.bigdata.flink.application

import com.bigdata.flink.common.TApplication
import com.bigdata.flink.controller.UniqueVistorAnalysisController

/**
 * Ciel
 * project-flink: com.bigdata.flink.application
 * 2020-07-02 22:51:59
 */
object UniqueVistorAnalysisByBloomFilterApplication extends App with TApplication {
    start("fileText") {
        val uniqueVistorAnalysisController = new UniqueVistorAnalysisController
        uniqueVistorAnalysisController.getUVByBloomFilter("datasources\\UserBehavior.csv")
    }
}
/**
 * (1511661600000,28196)
 * (1511665200000,32160)
 * (1511668800000,32233)
 * (1511672400000,30615)
 * (1511676000000,32747)
 * (1511679600000,33898)
 * (1511683200000,34631)
 * (1511686800000,34746)
 * (1511690400000,32356)
 * (1511694000000,13)
 */
package com.bigdata.flink.application

import com.bigdata.flink.common.TApplication
import com.bigdata.flink.controller.PageViewAnalysisController

/**
 * Ciel
 * project-flink: com.bigdata.flink.application
 * 2020-07-02 22:23:40
 */
object PageViewAnalysisApplication extends App with TApplication {
    start("fileText") {
        val pageViewAnalysisController = new PageViewAnalysisController
        pageViewAnalysisController.getPV("datasources\\UserBehavior.csv").print
    }


}

package com.bigdata.flink.application

import com.bigdata.flink.common.TApplication
import com.bigdata.flink.controller.UniqueVistorAnalysisController

/**
 * Ciel
 * project-flink: com.bigdata.flink.application
 * 2020-07-02 22:33:25
 */
object UniqueVistorAnalysisApplication extends App with TApplication{
    start("fileText"){
        val uniqueVistorAnalysisController = new UniqueVistorAnalysisController
        uniqueVistorAnalysisController.getUV("datasources\\UserBehavior.csv").print
    }

}

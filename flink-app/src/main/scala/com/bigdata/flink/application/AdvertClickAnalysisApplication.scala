package com.bigdata.flink.application

import com.bigdata.flink.common.TApplication
import com.bigdata.flink.controller.AdvertClickAnalysisController

/**
 * Ciel
 * project-flink: com.bigdata.flink.application
 * 2020-07-03 12:29:16
 */
object AdvertClickAnalysisApplication extends App with TApplication{
    start("fileText"){
        val advertClickAnalysisController = new AdvertClickAnalysisController
        advertClickAnalysisController.getAnalysisData("")
    }
}

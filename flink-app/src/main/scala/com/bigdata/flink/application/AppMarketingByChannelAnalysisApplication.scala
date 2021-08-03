package com.bigdata.flink.application

import com.bigdata.flink.common.TApplication
import com.bigdata.flink.controller.AppMarketingByChannelAnalysisController

/**
 * Ciel
 * project-flink: com.bigdata.flink.application
 * 2020-07-03 12:34:28
 */
object AppMarketingByChannelAnalysisApplication extends App with TApplication{
    start("fielText"){
        val appMarketingByChannelAnalysisController = new AppMarketingByChannelAnalysisController
        appMarketingByChannelAnalysisController.getAnalysisDatas().print
        
    }

}

package com.bigdata.flink.controller

import com.bigdata.flink.service.AppMarketingByChannelAnalysisService

/**
 * Ciel
 * project-flink: com.bigdata.flink.controller
 * 2020-07-03 12:35:46
 */
class AppMarketingByChannelAnalysisController {
    val appMarketingByChannelAnalysisService = new AppMarketingByChannelAnalysisService

    def getAnalysisDatas() = {
        appMarketingByChannelAnalysisService.getAnalysisDatas()
    }


}

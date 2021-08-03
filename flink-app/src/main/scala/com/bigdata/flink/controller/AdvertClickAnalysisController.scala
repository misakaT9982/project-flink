package com.bigdata.flink.controller

import com.bigdata.flink.service.AdvertClickAnalysisService

/**
 * Ciel
 * project-flink: com.bigdata.flink.controller
 * 2020-07-03 12:29:56
 */
class AdvertClickAnalysisController {
    val advertClickAnalysisService = new AdvertClickAnalysisService

    def getAnalysisData(source: String) = {
        advertClickAnalysisService.getAnalysisData(source)
    }


}

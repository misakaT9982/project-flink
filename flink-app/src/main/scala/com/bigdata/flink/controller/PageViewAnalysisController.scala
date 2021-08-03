package com.bigdata.flink.controller

import com.bigdata.flink.service.PageViewAnalysisService

/**
 * Ciel
 * project-flink: com.bigdata.flink.controller
 * 2020-07-02 22:24:14
 */
class PageViewAnalysisController {
    private val pageViewAnalysisService = new PageViewAnalysisService

    def getPV(source: String) = {
        pageViewAnalysisService.getPV(source)
    }


}

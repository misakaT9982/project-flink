package com.bigdata.flink.controller

import com.bigdata.flink.service.UniqueVistorAnalysisService

/**
 * Ciel
 * project-flink: com.bigdata.flink.controller
 * 2020-07-02 22:34:20
 */
class UniqueVistorAnalysisController {

    private val uniqueVistorAnalysisService = new UniqueVistorAnalysisService

    def getUV(source: String) = {
        uniqueVistorAnalysisService.getUV(source)

    }
    def getUVByBloomFilter(source: String): Unit = {
        uniqueVistorAnalysisService.getUVByBloomFilter(source)
    }


}

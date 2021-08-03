package com.bigdata.flink.controller

import com.bigdata.flink.service.{OrderTimeoutAnalysisWithCEPService, OrderTimeoutAnalysisWithOutCEPService}
import org.apache.flink.streaming.api.scala.DataStream


/**
 * Ciel
 * project-flink: com.bigdata.flink.controller
 * 2020-07-03 11:41:43
 */
class OrderTimeoutAnalysisController {
    private val orderTimeoutAnalysisWithOutCEPService = new OrderTimeoutAnalysisWithOutCEPService
    private val orderTimeoutAnalysisWithCEPService = new OrderTimeoutAnalysisWithCEPService


    def getOrderTimeOutWithOutCEPDatas(source: String): DataStream[String] = {
        orderTimeoutAnalysisWithOutCEPService.getOrderTimeOutWithOutCEPDatas(source)
    }

    def getOrderTimeOutWithCEPDatas(source: String): DataStream[String] = {
        orderTimeoutAnalysisWithCEPService.getOrderTimeOutWithCEPDatas(source)
    }


}

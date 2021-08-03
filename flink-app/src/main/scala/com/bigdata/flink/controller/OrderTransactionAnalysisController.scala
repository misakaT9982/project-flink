package com.bigdata.flink.controller

import com.bigdata.flink.bean
import com.bigdata.flink.service.{OrderTransactionAnalysisByJoinService, OrderTransactionAnalysisService}
import org.apache.flink.streaming.api.scala.DataStream

/**
 * Ciel
 * project-flink: com.bigdata.flink.controller
 * 2020-07-03 13:57:02
 */
class OrderTransactionAnalysisController {
    val orderTransactionAnalysisService = new OrderTransactionAnalysisService
    private val orderTransactionAnalysisByJoinService = new OrderTransactionAnalysisByJoinService

    def getOrderTransactionDatas(source1: String, source2: String) = {
        orderTransactionAnalysisService.getOrderTransactionDatas(source1, source2)
    }
    def getOrderTransactionByJoinDatas(source1: String, source2: String): DataStream[(bean.OrderLogData, bean.TXLogData)] = {
        orderTransactionAnalysisByJoinService.getOrderTransactionByJoinDatas(source1, source2)
    }


}

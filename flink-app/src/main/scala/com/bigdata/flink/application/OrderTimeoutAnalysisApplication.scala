package com.bigdata.flink.application

import com.bigdata.flink.common.TApplication
import com.bigdata.flink.controller.OrderTimeoutAnalysisController

/**
 * Ciel
 * project-flink: com.bigdata.flink.application
 * 2020-07-03 11:40:54
 */
object OrderTimeoutAnalysisApplication extends App with TApplication {
    start("fileText"){
        val orderTimeoutAnalysisController = new OrderTimeoutAnalysisController
        orderTimeoutAnalysisController
          //.getOrderTimeOutWithOutCEPDatas("datasources\\OrderLog.csv")
          .getOrderTimeOutWithCEPDatas("datasources\\OrderLog.csv")
          .print()
    }
}

package com.bigdata.flink.application

import com.bigdata.flink.common.TApplication
import com.bigdata.flink.controller.OrderTransactionAnalysisController

/**
 * Ciel
 * project-flink: com.bigdata.flink.application
 * 2020-07-03 13:49:30
 */
object OrderTransactionAnalysisApplication extends App with TApplication {
    start("fileText") {
       val  orderTransactionAnalysisController = new OrderTransactionAnalysisController

        // orderTransactionAnalysisController.getOrderTransactionDatas("datasources\\OrderLog.csv","datasources\\ReceiptLog.csv")

        orderTransactionAnalysisController
          .getOrderTransactionByJoinDatas("datasources\\OrderLog.csv","datasources\\ReceiptLog.csv")
          .print()
    }
}

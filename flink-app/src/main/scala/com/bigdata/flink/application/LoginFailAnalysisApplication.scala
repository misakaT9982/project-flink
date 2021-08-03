package com.bigdata.flink.application

import com.bigdata.flink.common.TApplication
import com.bigdata.flink.controller.LoginFailAnalysisController

/**
 * Ciel
 * project-flink: com.bigdata.flink.application
 * 2020-07-03 11:08:58
 */
object LoginFailAnalysisApplication extends App with TApplication{
    start("fileText"){
        val loginFailAnalysisController = new LoginFailAnalysisController
        loginFailAnalysisController.getLoginFailDatas(3,"datasources\\LoginLog.csv")
          .print()
    }
}

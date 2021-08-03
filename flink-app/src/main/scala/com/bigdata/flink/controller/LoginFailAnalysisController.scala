package com.bigdata.flink.controller

import com.bigdata.flink.service.LoginFailAnalysisService

/**
 * Ciel
 * project-flink: com.bigdata.flink.controller
 * 2020-07-03 11:10:12
 */
class LoginFailAnalysisController {
    private val loginFailAnalysisService = new LoginFailAnalysisService
    def getLoginFailDatas(rankNum: Int, source: String) = {
        loginFailAnalysisService.getLoginFailDatas(rankNum,source)
    }


}

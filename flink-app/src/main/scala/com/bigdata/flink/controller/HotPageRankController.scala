package com.bigdata.flink.controller

import com.bigdata.flink.service.HotPageRankService

/**
 * Ciel
 * project-flink: com.bigdata.flink.controller
 * 2020-07-02 21:42:51
 */
class HotPageRankController {
    private val hotPageRankService = new HotPageRankService

    def getHotPageRank(rankNum: Int, source: String) = {
        hotPageRankService.getHotPageRank(rankNum,source)
    }


}

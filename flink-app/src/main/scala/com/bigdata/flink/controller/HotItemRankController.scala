package com.bigdata.flink.controller

import com.bigdata.flink.service.HotItemRankService
import org.apache.flink.streaming.api.scala.DataStream

/**
 * Ciel
 * project-flink: com.bigdata.flink.controller
 * 2020-07-02 18:42:37
 */
class HotItemRankController {
    def getHotItemRank(rankNum: Int, source: String): DataStream[String] = {
        val hotItemRankService = new HotItemRankService
        hotItemRankService.getHotItemRank(rankNum,source)
    }

}

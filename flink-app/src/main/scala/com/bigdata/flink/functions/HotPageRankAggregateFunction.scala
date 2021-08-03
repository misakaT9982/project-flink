package com.bigdata.flink.functions

import com.bigdata.flink.bean.ApacheLogEvent
import org.apache.flink.api.common.functions.AggregateFunction

/**
 * Ciel
 * project-flink: com.bigdata.flink.functions
 * 2020-07-02 21:54:36
 */
class HotPageRankAggregateFunction extends AggregateFunction[ApacheLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1L

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}

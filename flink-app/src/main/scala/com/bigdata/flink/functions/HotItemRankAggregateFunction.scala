package com.bigdata.flink.functions

import com.bigdata.flink.bean.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction


/**
 * Ciel
 * project-flink: com.bigdata.flink.functions
 * 2020-07-02 19:03:34
 */
class HotItemRankAggregateFunction extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehavior, accumulator: Long): Long = {
        accumulator + 1L
    }

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}

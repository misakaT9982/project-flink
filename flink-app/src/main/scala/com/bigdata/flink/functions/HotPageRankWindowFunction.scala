package com.bigdata.flink.functions

import java.lang

import com.bigdata.flink.bean.URLClickCount
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * Ciel
 * project-flink: com.bigdata.flink.functions
 * 2020-07-02 21:53:44
 */
class HotPageRankWindowFunction extends WindowFunction[Long, URLClickCount, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[URLClickCount]): Unit = {
        out.collect(URLClickCount(key, input.iterator.next(), window.getEnd))
    }
}

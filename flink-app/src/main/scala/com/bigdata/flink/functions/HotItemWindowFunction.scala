package com.bigdata.flink.functions

import com.bigdata.flink.bean.ItemClickCount
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * Ciel
 * project-flink: com.bigdata.flink.functions
 * 2020-07-02 19:06:42
 */
class HotItemWindowFunction extends WindowFunction[Long, ItemClickCount, Long, TimeWindow] {
    override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemClickCount]): Unit = {
        out.collect(ItemClickCount(key,input.iterator.next(),window.getEnd))
    }
}

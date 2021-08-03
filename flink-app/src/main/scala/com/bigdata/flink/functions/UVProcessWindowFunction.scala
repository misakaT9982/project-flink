package com.bigdata.flink.functions

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
 * Ciel
 * project-flink: com.bigdata.flink.functions
 * 2020-07-02 22:39:45
 */
class UVProcessWindowFunction extends ProcessWindowFunction[(String, Long), (String, Long), String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[(String, Long)]): Unit = {
        val set = mutable.Set[Long]()
        for (data <- elements.iterator) {
            set.add(data._2)
        }
        out.collect(("" + context.window.getEnd, set.size))
    }
}

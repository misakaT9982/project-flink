package com.bigdata.flink.functions

import java.sql.Timestamp

import com.bigdata.flink.bean.URLClickCount
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * Ciel
 * project-flink: com.bigdata.flink.functions
 * 2020-07-02 21:59:23
 */
class HotPageRankKeyedProcessFunction(num: Int) extends KeyedProcessFunction[Long, URLClickCount, String] {

    private var urlDatas: ListState[URLClickCount] = _
    private var alarmTimer: ValueState[Long] = _

    override def open(parameters: Configuration): Unit = {
        urlDatas = getRuntimeContext
          .getListState(
              new ListStateDescriptor[URLClickCount]("urlDatas", classOf[URLClickCount])
          )
        alarmTimer = getRuntimeContext
          .getState(
              new ValueStateDescriptor[Long]("alarmTimer", classOf[Long])
          )
    }

    // 将每一次进入分区的数据进行状态的保存
    override def processElement(value: URLClickCount, ctx: KeyedProcessFunction[Long, URLClickCount, String]#Context, out: Collector[String]): Unit = {
        urlDatas.add(value)
        if (alarmTimer.value() == 0) {
            ctx.timerService().registerEventTimeTimer(value.windowEndTime)
            alarmTimer.update(value.windowEndTime)
        }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, URLClickCount, String]#OnTimerContext, out: Collector[String]): Unit = {
        import scala.collection.JavaConversions._
        val buffer = new ListBuffer[URLClickCount]
        val counts = urlDatas.get()
        for (ele <- counts.iterator()) {
            buffer.append(ele)
        }
        urlDatas.clear()

        val result = buffer.sortWith(
            (ur1, ur2) => {
                //按照访问量从大到小排序
                ur1.count > ur2.count
            }).take(num)

        val builder = new StringBuilder
        builder.append("时间范围：" + new Timestamp(timestamp) + "\n")

        result.foreach(data => {
            builder.append("URL: " + data.url + ", 浏览次数: " + data.count + "\n")
        })

        builder.append("==========================")

        Thread.sleep(1000)
        out.collect(builder.toString())
    }
}

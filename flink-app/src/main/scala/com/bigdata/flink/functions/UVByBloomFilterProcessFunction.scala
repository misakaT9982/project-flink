package com.bigdata.flink.functions

import com.bigdata.flink.util.BloomFilter
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
 * Ciel
 * project-flink: com.bigdata.flink.functions
 * 2020-07-02 23:00:36
 */
class UVByBloomFilterProcessFunction extends ProcessWindowFunction[(String, Long), (String, Long), String, TimeWindow] {

    private var jedis: Jedis = _

    override def open(parameters: Configuration): Unit = {
        jedis = new Jedis("realtime102",6379)
    }

    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[(String, Long)]): Unit = {
        val storeKey: String = context.window.getEnd.toString
        var count = 0L
        if(jedis.hget("uvcount",storeKey) != null){
            count = jedis.hget("uvcount", storeKey).toLong
        }
        // 使用redis的位图操作 : key, offset
        val offset = BloomFilter.offset(elements.iterator.next()._2.toString, 59)
        // getbit --> 取位图
        if(jedis.getbit(storeKey,offset)){
            // 用户ID可能存在
            out.collect(storeKey,count)
        }else{
            // 用户ID一定不存在
            // 在redis中更新用户ID所在位图的状态
            jedis.setbit(storeKey,offset,true)
            // 在redis中统计uvcount
            jedis.hset("uvcount",storeKey,(count+1).toString)
            out.collect((storeKey,count + 1))
        }
    }
}

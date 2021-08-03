package com.bigdata.flink.util


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Ciel
 * project-flink: com.bigdata.flink.util
 * 2020-07-02 18:47:07
 */
object FlinkStreamEnv {
    private val locals = new ThreadLocal[StreamExecutionEnvironment]

    def init(): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        locals.set(env)
    }

    def execute(): Unit = {
        locals.get().execute()
    }

    def get(): StreamExecutionEnvironment = {
        locals.get()
    }

}

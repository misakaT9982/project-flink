package com.bigdata.flink.common

import com.bigdata.flink.util.FlinkStreamEnv

/**
 * Ciel
 * project-flink: com.bigdata.flink.common
 * 2020-07-02 18:40:10
 */
trait TApplication {
    def start(sourceType: String)(op: => Unit): TApplication = {
        try {
            FlinkStreamEnv.init()
            op
            FlinkStreamEnv.execute()
        } catch {
            case e => e.printStackTrace()
        }
        this
    }

    def close(op: => Unit): Unit = {
        op
    }
}

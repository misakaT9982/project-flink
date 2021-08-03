package com.bigdata.flink.common

import com.bigdata.flink.util.FlinkStreamEnv
import org.apache.flink.streaming.api.scala.DataStream

/**
 * Ciel
 * project-flink: com.bigdata.flink.common
 * 2020-07-02 18:45:41
 */
trait TDao {

    def readTextFile(dataPath: String): DataStream[String] = {
        FlinkStreamEnv.get().readTextFile(dataPath)
    }
}

package com.bigdata.flink.service

import com.bigdata.flink.bean.LoginLogData
import com.bigdata.flink.dao.LoginFailAnalysisDao
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * Ciel
 * project-flink: com.bigdata.flink.service
 * 2020-07-03 11:14:55
 */
class LoginFailAnalysisService {
    private val loginFailAnalysisDao = new LoginFailAnalysisDao

    def getLoginFailDatas(rankNum: Int, source: String): DataStream[String] = {
        val fileDS: DataStream[String] = loginFailAnalysisDao.readTextFile(source)
        val dataDS = fileDS.map(line => {
            val datas = line.split(",")
            LoginLogData(datas(0).toLong, datas(1), datas(2), datas(3).toLong)
        })

        val tsDS = dataDS.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor[LoginLogData](Time.seconds(10)) {
                override def extractTimestamp(element: LoginLogData): Long = {
                    element.timestamp * 1000
                }
            })
        val dataKS = tsDS
          .filter(_.status == "fail")
          .keyBy(_.userId)
        dataKS.process(
            new KeyedProcessFunction[Long, LoginLogData, String] {
                private var lastLoginData: ValueState[LoginLogData] = _

                override def open(parameters: Configuration): Unit = {
                    lastLoginData = getRuntimeContext
                        .getState(
                            new ValueStateDescriptor[LoginLogData]("lastLoginData",classOf[LoginLogData])
                        )
                }

                override def processElement(value: LoginLogData, ctx: KeyedProcessFunction[Long, LoginLogData, String]#Context, out: Collector[String]): Unit = {
                    val lastLogin = lastLoginData.value()
                    if(lastLogin != null){
                        val time1 = lastLogin.timestamp
                        val time2 = value.timestamp
                        if(time2 - time1 <= 2){
                            out.collect(ctx.getCurrentKey + "在2秒（" + time2 + ", " + time1 + "）内连续登陆失败2次")
                        }
                    }
                    lastLoginData.update(value)
                }
            }
        )
    }
}

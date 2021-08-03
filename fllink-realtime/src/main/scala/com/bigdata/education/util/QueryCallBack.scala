package com.bigdata.education.util

import java.sql.ResultSet

/**
 * Ciel
 * project-flink: com.bigdata.education.util
 * 2020-08-25 09:43:46
 */
trait QueryCallBack {
        def process(rs: ResultSet)
}

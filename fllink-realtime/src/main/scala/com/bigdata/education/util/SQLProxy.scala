package com.bigdata.education.util

import java.sql.{Connection, PreparedStatement, ResultSet}

/**
 * Ciel
 * project-flink: com.bigdata.education.util
 * 2020-08-25 09:29:49
 */
class SQLProxy {
    private var rs: ResultSet = _
    private var psmt: PreparedStatement = _

    /**
     * 执行修改语句
     *
     * @param conn
     * @param sql
     * @param params
     * @return
     */
    def executeUpdata(conn: Connection, sql: String, params: Array[Any]): Int = {
        var rtn = 0
        try {
            psmt = conn.prepareStatement(sql)
            if (params != null && params.length > 0) {
                for (i <- 0 until params.length) {
                    psmt.setObject(i + 1, params(i))
                }
            }
            rtn = psmt.executeUpdate()
        } catch {
            case e: Exception => e.printStackTrace()
        }
        rtn
    }

    /**
     * 执行查询语句
     *
     * @param conn
     * @param sql
     * @param params
     * @param queryCallBack
     */
    def executeQuery(conn: Connection, sql: String, params: Array[Any], queryCallBack: QueryCallBack): Unit = {
        rs = null
        try {
            psmt = conn.prepareStatement(sql)
            if (params != null && params.length > 0) {
                for (i <- 0 until params.length) {
                    psmt.setObject(i + 1, params(i))
                }
            }
            rs = psmt.executeQuery()
            queryCallBack.process(rs)
        } catch {
            case e: Exception => e.printStackTrace()
        }
    }

    def shutDown(conn: Connection):Unit = DataSourceUtil.closeResource(rs,psmt,conn)

}

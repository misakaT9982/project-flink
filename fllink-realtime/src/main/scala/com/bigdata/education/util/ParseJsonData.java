package com.bigdata.education.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * Ciel
 * project-flink: com.bigdata.education.util
 * 2020-08-25 09:26:11
 */
public class ParseJsonData {
    public static JSONObject getJsonData(String data){
        try {
            return JSONObject.parseObject(data);
        }catch (Exception e){
            return null;
        }
    }
    public static String getJsonString(Object o){
        return JSON.toJSONString(o);
    }
}

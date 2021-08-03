package com.bigdata.education.util;

import java.io.InputStream;
import java.util.Properties;

/**
 * Ciel
 * project-flink: com.bigdata.education.util
 * 2020-08-25 09:18:54
 */
public class ConfigurationManager {
    private static Properties pros = new Properties();

    static {
        try {
            InputStream input = ConfigurationManager
                    .class.getClassLoader()
                    .getResourceAsStream("comerce.properties");
            pros.load(input);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 获取配置项
    public static String getProperty(String key) {
        return pros.getProperty(key);
    }

    // 获取布尔型的配置项
    public static Boolean getBoolean(String key){
        String value = pros.getProperty(key);
        try{
            return Boolean.valueOf(value);
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }
}

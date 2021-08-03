package com.bigdata.education.kafkaproducer;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class BaseWebSiteKafkaProducer {
    public static void main(String[] args) {
        Properties pros = new Properties();
        pros.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        pros.put("acks", "-1");
        pros.put("batch.size", "16384");
        pros.put("linger.ms", "10");
        pros.put("buffer.memory", "33554432");
        pros.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        pros.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(pros);
        for (int i = 0; i < 5; i++) {
            GdmBaseWebsite gdmBaseWebsite = GdmBaseWebSieLog.generateLog(String.valueOf(i), "webA");
            String jsonString = JSON.toJSONString(gdmBaseWebsite);
            producer.send(new ProducerRecord<String, String>("basewebsite", jsonString));
        }
        for (int i = 0; i < 5; i++) {
            GdmBaseWebsite gdmBaseWebsite = GdmBaseWebSieLog.generateLog(String.valueOf(i), "webB");
            String jsonString = JSON.toJSONString(gdmBaseWebsite);
            producer.send(new ProducerRecord<String, String>("basewebsite", jsonString));
        }
        for (int i = 0; i < 5; i++) {
            GdmBaseWebsite gdmBaseWebsite = GdmBaseWebSieLog.generateLog(String.valueOf(i), "webC");
            String jsonString = JSON.toJSONString(gdmBaseWebsite);
            producer.send(new ProducerRecord<String, String>("basewebsite", jsonString));
        }
        producer.flush();
        producer.close();
    }
    public static class GdmBaseWebsite {

        private String siteid;
        private String sitename;
        private String siteurl;
        private String creator;
        private String createtime;
        private String delete;
        private String dn;

        public String getSiteid() {
            return siteid;
        }

        public void setSiteid(String siteid) {
            this.siteid = siteid;
        }

        public String getSitename() {
            return sitename;
        }

        public void setSitename(String sitename) {
            this.sitename = sitename;
        }

        public String getSiteurl() {
            return siteurl;
        }

        public void setSiteurl(String siteurl) {
            this.siteurl = siteurl;
        }

        public String getCreator() {
            return creator;
        }

        public void setCreator(String creator) {
            this.creator = creator;
        }

        public String getCreatetime() {
            return createtime;
        }

        public void setCreatetime(String createtime) {
            this.createtime = createtime;
        }

        public String getDelete() {
            return delete;
        }

        public void setDelete(String delete) {
            this.delete = delete;
        }

        public String getDn() {
            return dn;
        }

        public void setDn(String dn) {
            this.dn = dn;
        }
    }
    public static class GdmBaseWebSieLog {

        private static String[] siteNames = new String[]{"百度", "163", "114", "126", "谷歌"};
        private static String[] siteUrls = new String[]{"wwww.baidu.com", "www.163.com", "www.114.com",
                "www.126.com", "www.google.com"};

        public static GdmBaseWebsite generateLog(String siteid, String dn) {
            GdmBaseWebsite baseWebsite = new GdmBaseWebsite();
            Random rand = new Random();
            baseWebsite.setCreatetime("2000-01-01");
            baseWebsite.setDelete("0");
            baseWebsite.setSiteid(siteid);
            baseWebsite.setCreator("admin");
            baseWebsite.setDn(dn);
            int index = Integer.parseInt(siteid);
            baseWebsite.setSitename(siteNames[index]);
            baseWebsite.setSiteurl(siteUrls[index] + "/" + dn);
            return baseWebsite;
        }
    }
}

package com.bigdata.education.kafkaproducer;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Ciel
 * project-flink: com.bigdata.education.kafkaproducer
 * 2020-08-24 18:27:34
 */
public class BaseAdLogKafkaProducer {
    public static void main(String[] args) {
        Properties pros = new Properties();
        pros.put("bootstrap.service","hadoop102:9092,hadoop103:9092,hadoop104:9092");
        pros.put("acks","-1");
        pros.put("batch.size","16384");
        pros.put("linger.ms","10");
        pros.put("buffer.memory","33554432");
        pros.put("key.serialzier","org.apache.kafka.common.serialization.StringSerializer");
        pros.put("value.serialzier","org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(pros);
        for (int i = 0; i < 10; i++) {
            GdmBaseAd gdmBaseAd = GdmBaseAdLog.generateLog(String.valueOf(i));
            String jsonString = JSON.toJSONString(gdmBaseAd);
            producer.send(new ProducerRecord<String,String>("baseString",jsonString));
        }
        producer.flush();
        producer.close();
    }

    public static class GdmBaseAd{
        private String adid;
        private String adname;
        private String dn;

        public String getAdid() {
            return adid;
        }

        public void setAdid(String adid) {
            this.adid = adid;
        }

        public String getAdname() {
            return adname;
        }

        public void setAdname(String adname) {
            this.adname = adname;
        }

        public String getDn() {
            return dn;
        }

        public void setDn(String dn) {
            this.dn = dn;
        }
    }

    public static class GdmBaseAdLog{
        public static GdmBaseAd generateLog(String adid){
            GdmBaseAd gdmBaseAd = new GdmBaseAd();
            gdmBaseAd.setAdid(adid);
            gdmBaseAd.setAdname("注册弹窗广告" + adid);
            gdmBaseAd.setDn("webA");
            return gdmBaseAd;
        }
    }
}

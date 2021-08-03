package com.bigdata.education.kafkaproducer;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Ciel
 * project-flink: com.bigdata.education.kafkaproducer
 * 2020-08-24 18:59:41
 */
public class BaseMemberPayMoney {
    public static BlockingQueue<KafkaProducer<String,String>> queue = new LinkedBlockingDeque<KafkaProducer<String, String>>(10);
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
        for (int i = 0; i < 7000000; i++) {
            GdmPcenterMemPaymoney memPaymoney = GdmPcenterMemPayMoneyLog.generateLog();
            String jsonString = JSON.toJSONString(memPaymoney);
            producer.send(new ProducerRecord<String, String>("memberpaymoney", jsonString));
        }
        producer.flush();
        producer.close();
    }

    public static class GdmPcenterMemPaymoney{
        private String uid;
        private String paymoney;
        private String vip_id;
        private String updatetime;
        private String siteid;
        private String dt;
        private String dn;
        private String createtime;

        public String getUid() {
            return uid;
        }

        public void setUid(String uid) {
            this.uid = uid;
        }

        public String getPaymoney() {
            return paymoney;
        }

        public void setPaymoney(String paymoney) {
            this.paymoney = paymoney;
        }

        public String getVip_id() {
            return vip_id;
        }

        public void setVip_id(String vip_id) {
            this.vip_id = vip_id;
        }

        public String getUpdatetime() {
            return updatetime;
        }

        public void setUpdatetime(String updatetime) {
            this.updatetime = updatetime;
        }

        public String getSiteid() {
            return siteid;
        }

        public void setSiteid(String siteid) {
            this.siteid = siteid;
        }

        public String getDt() {
            return dt;
        }

        public void setDt(String dt) {
            this.dt = dt;
        }

        public String getDn() {
            return dn;
        }

        public void setDn(String dn) {
            this.dn = dn;
        }

        public String getCreatetime() {
            return createtime;
        }

        public void setCreatetime(String createtime) {
            this.createtime = createtime;
        }
    }

    public static class GdmPcenterMemPayMoneyLog{
        public static GdmPcenterMemPaymoney generateLog(){
            GdmPcenterMemPaymoney memPayMoney = new GdmPcenterMemPaymoney();
            Random random = new Random();
            double money = random.nextDouble() * 1000;
            DecimalFormat df = new DecimalFormat("0.00");
            memPayMoney.setPaymoney(df.format(money));
            memPayMoney.setDt(DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now().minusDays(8)));
            memPayMoney.setDn("webA");
            memPayMoney.setSiteid(String.valueOf(random.nextInt(5)));
            memPayMoney.setVip_id(String.valueOf(random.nextInt(5)));
            memPayMoney.setUid(String.valueOf(random.nextInt(5000000)));
            memPayMoney.setCreatetime(String.valueOf(System.currentTimeMillis()));
            return memPayMoney;
        }
    }
}

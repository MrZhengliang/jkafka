package com.kafka.service;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * kafka生产者服务
 * Created by xx on 2017/6/5.
 */
public class KafkaProduceService {
    private static Logger logger = LoggerFactory.getLogger(KafkaProduceService.class);

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers","192.168.0.15:9092,192.168.0.16:9092,192.168.0.34:9092");
        props.put("retries",3);
        props.put("linger.ms",1);
//        props.put("serializer.class","kafka.serializer.StringEncoder");
//        props.put("partitioner.class", "SimplePartitioner");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");


        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 3; i++) {
            ProducerRecord<String,String> record = new ProducerRecord<String, String>(
                    "test", "11","今天天气下雨-----"+i
            );
            producer.send(record, new Callback() {
                        public void onCompletion(RecordMetadata metadata, Exception e) {
                            if (e != null) {
                                System.out.println("the producer has a error:"
                                        + e.getMessage());
                            }else {
                                System.out
                                        .println("The offset of the record we just sent is: "
                                                + metadata.offset());
                                System.out
                                        .println("The partition of the record we just sent is: "
                                                + metadata.partition());
                            }
                        }
                    }
            );

            try {
                Thread.sleep(1000);
//                producer.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}



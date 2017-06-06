package com.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * kafka消费者
 * 自己控制偏移量提交
 * 很多时候，我们是希望在获得消息并经过一些逻辑处理后，
 * 才认为该消息已被消费，这可以通过自己控制偏移量提交来实现
 * Created by xx on 2017/6/5.
 */
public class KafkaConsumeService {

    private static Logger logger = LoggerFactory.getLogger(KafkaConsumeService.class);


    public static void main(String[] args) {
        Properties props = new Properties();
        // 设置brokerServer(kafka)ip地址
        props.put("bootstrap.servers","192.168.0.15:9092,192.168.0.16:9092,192.168.0.34:9092");
        System.out.println("this is the group part test 1");
        // 设置consumer group name
//        props.put("group.id","mygroup11");
        props.put("group.id", "1111");
        props.put("enable.auto.commit", "false");
        // 设置使用最开始的offset偏移量为该group.id的最早。如果不设置，则会是latest即该topic最新一个消息的offset
        // 如果采用latest，消费者只能得道其启动后，生产者生产的消息
        props.put("auto.offset.reset","earliest");
        // 设置心跳时间
        props.put("session.timeout.ms","30000");
        // 编解码
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("test"));

        final int minBatchSize = 5; // 批量提交数量

        List<ConsumerRecord<String,String>> buffer = new ArrayList<ConsumerRecord<String, String>>();

        while(true){
            ConsumerRecords<String,String> records = consumer.poll(1000);
            for(ConsumerRecord<String,String> record : records){
                //　正常这里应该使用线程池处理，不应该在这里处理
                System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value()+"\n");
                System.out.println("consumer message values is " +record.value() + ",and the offset is " +record.offset());
                buffer.add(record);
            }

            if(buffer.size() >= minBatchSize){
                System.out.println("now commit offset"+buffer.size());
                consumer.commitSync();
                buffer.clear();
            }
        }
    }
}

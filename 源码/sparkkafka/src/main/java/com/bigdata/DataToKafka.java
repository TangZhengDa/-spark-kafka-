package com.bigdata;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 数据写入kafka
 */
public class DataToKafka {

    private static String BeforeFilterTopicName = "result";
    private static String kafkaClusterIP = "172.26.212.114:9092";
    private static String recordFilePath = "./output/result.txt";


    static void jsonToKafka() throws IOException {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaClusterIP);//kafka clusterIP
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        BufferedReader br =  new BufferedReader(new FileReader(recordFilePath));
        String record;
        //send record to kafka
        int i=0;
        while((record = br.readLine())!=null) {
            producer.send(new ProducerRecord<String, String>(BeforeFilterTopicName, Integer.toString(i++), record), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null)
                        e.printStackTrace();
                    System.out.println("The offset of the record we just sent is: " + metadata.offset());
                }
            });
        }
        producer.close();
    }


    public static void main(String[] args) throws IOException {
        //发送record至kafka
        jsonToKafka();
    }
}

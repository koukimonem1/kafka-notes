package com.lansrod.kafka.consumer;

import java.util.Calendar;
import java.util.Collections;
import java.util.Properties;

import javax.xml.datatype.Duration;
import javax.xml.datatype.DatatypeConstants.Field;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

// #### We call the action of updating the current position in the partition a commit .
// #### Consumers commit offsets by producing message to a special topic named __consumer_offsets with the committed offset for each partition
// #### In order to know where to pick up the work, the consumer will read the latest committed offset of each partition and continue from there

// !!!!!! If the committed offset is smaller than the offset of the last message the client processed, the messages between the last processed offset and the committed offset will
// be processed twice

// !!!!!! If the committed offset is larger than the offset of the last message the client actually processed, all messages between the last processed offset and the committed offset
// will be missed by the consumer group
public class Consumer3CommitSync {
	public static void main(String[] args) {
		Properties configs = new Properties();
		configs.put("bootstrap.servers", "localhost:9092");
		configs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		configs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		configs.put("group.id", "group2");

		/****************************************************
		 **************** AUTO COMMITS ***********************
		 *****************************************************/
//If you configure enable.auto.commit=true , then every five seconds (default interval) the consumer will commit the largest offset your client received from poll()
		// configs.put("enable.auto.commit", true);
// We can change this default interval
		// configs.put("auto.commit.interval.ms", 10);

		/****************************************************
		 **************** COMMOIT CURRENT OFFSET **************
		 *****************************************************/
//offsets will only be committed when the application explicitly chooses to do so
		configs.put("auto.commit.offset", false);
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
		consumer.subscribe(Collections.singleton("koukitest"));
		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(java.time.Duration.ofMillis(1000));
				for (ConsumerRecord<String, String> record : records) {
					System.out.println("topic = " + record.topic() + ";offset = " + record.offset() + ";key = "
							+ record.key() + ";value = " + record.value());
				}
				try {
					consumer.commitSync();
				} catch (CommitFailedException e) {
					e.printStackTrace();
				}

			}
		} finally {
			consumer.close();
		}

	}
}

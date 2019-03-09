package com.lansrod.kafka.consumer;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

public class Consumer4CommitAsynchronousCommit2 {

	public static void main(String[] args) {
		Properties configs = new Properties();
		configs.put("bootstrap.servers", "localhost:9092");
		configs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		configs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		configs.put("group.id", "group2");
		configs.put("auto.commit.offset", false);

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("topic = %s, partition = %s,offset = %d, customer = %s, country = %s\n",
						record.topic(), record.partition(), record.offset(), record.key(), record.value());
			}
			// Callback will be triggered when the broker responds.
			consumer.commitAsync(new OffsetCommitCallback() {
				public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
					if (exception != null)
						// We send the commit and carry on, but if the commit fails, the failure and the
						// offsets will be logged
						exception.printStackTrace();
				}
			});
		}
	}

}

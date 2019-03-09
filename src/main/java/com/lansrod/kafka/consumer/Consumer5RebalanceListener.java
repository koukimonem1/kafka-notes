package com.lansrod.kafka.consumer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

//a consumer will want to do some cleanup work before exiting and also before partition rebalancing
//The consumer API allows you to run your own code when partitions are added or removed from the consumer.

public class Consumer5RebalanceListener {

	public static void main(String[] args) {
		Properties configs = new Properties();
		configs.put("bootstrap.servers", "localhost:9092");
		configs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		configs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		configs.put("group.id", "group2");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
		// we assign the listener here
		consumer.subscribe(Collections.singleton("koukitest"),
				(new Consumer5RebalanceListener()).new HandleRebalance());

		Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> record : records) {
					System.out.printf("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n",
							record.topic(), record.partition(), record.offset(), record.key(), record.value());
					currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
							new OffsetAndMetadata(record.offset() + 1, "no metadata"));
				}
				consumer.commitAsync(currentOffsets, null);
			}
		} catch (WakeupException e) {
			// ignore, we're closing
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				consumer.commitSync(currentOffsets);
			} finally {
				consumer.close();
				System.out.println("Closed consumer and we are done");
			}
		}
	}

	private class HandleRebalance implements ConsumerRebalanceListener {

		@Override
		public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			// TODO Auto-generated method stub

		}

		@Override
		public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			// TODO Auto-generated method stub

		}
	}
}

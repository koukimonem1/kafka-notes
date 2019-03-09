package com.lansrod.kafka.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

// if poll() returns a huge batch and you want to commit offsets in the middle of the batch to avoid having to process all those rows again if a rebalance occurs
// :( !!!!! You can’t just call commitSync() or commitAsync() —this will commit the last offset returned, which you didn’t get to process yet
// ===> The consumer API allows to pass a map of partitions and offsets that you wish to commit

public class Consumer4CommitAsynchronousCommit4 {
	public static void main(String[] args) {
		Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
		int count = 0;
		Properties configs = new Properties();
		configs.put("bootstrap.servers", "localhost:9092");
		configs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		configs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		configs.put("group.id", "group2");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
		consumer.subscribe(Collections.singleton("koukitest"));
		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
				for (ConsumerRecord<String, String> record : records) {
					System.out.printf("topic = %s, partition = %s, offset = %d,customer = %s, country = %s\n",
							record.topic(), record.partition(), record.offset(), record.key(), record.value());
					currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
							new OffsetAndMetadata(record.offset() + 1, "no metadata"));
					if (count % 1000 == 0)
						consumer.commitAsync(currentOffsets, null);
					count++;
				}
			}
		} finally {
			consumer.close();
		}

	}

}

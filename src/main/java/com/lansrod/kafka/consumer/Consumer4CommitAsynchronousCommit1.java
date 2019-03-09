package com.lansrod.kafka.consumer;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

// One drawback of manual commit is that the application is blocked until the broker responds to the commit request.
// ==> This will limit the throughput of the application
// Throughput can be improved by committing less frequently, but then we are increasing the number of potential duplicates that a rebalance will create

// !!!!! The drawback is that while commitSync() will retry the commit until it either succeeds or encounters a non-retriable failure, commitAsync() will not retry

// !!! Il faut bien lire la paragraphe ci-dessous

//Imagine that we sent a request to commit offset 2000. There is a temporary communication problem, so the broker never gets the request and therefore never responds. Meanwhile,
//we processed another batch and successfully committed offset 3000. If commitAsync() now retries the previously failed commit, it might succeed in committing offset 2000 after
//offset 3000 was already processed and committed. In the case of a rebalance, this will cause more duplicates.

/** see #Consumer4CommitAsynchronousCommit2 */

public class Consumer4CommitAsynchronousCommit1 {

	public static void main(String[] args) {
		Properties configs = new Properties();
		configs.put("bootstrap.servers", "localhost:9092");
		configs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		configs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		configs.put("group.id", "group2");
		configs.put("auto.commit.offset", false);

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("topic = %s, partition = %s,offset = %d, customer = %s, country = %s\n",
						record.topic(), record.partition(), record.offset(), record.key(), record.value());
			}
			consumer.commitAsync();
		}
	}

}

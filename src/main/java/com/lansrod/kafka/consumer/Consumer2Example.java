package com.lansrod.kafka.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

// **** You can’t have multiple consumers that belong to the same group in one thread 
// **** you can’t have multiple threads safely use the same consumer. 
// ===> One consumer per thread is the rule. 
// ===> To run multiple consumers in the same group in one application, you will need to run each in its own thread.

public class Consumer2Example {
	public static void main(String[] args) {
		Properties configs = new Properties();

		// Mandatory configuration
		configs.put("bootstrap.servers", "localhost:9092");
		configs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		configs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		// Consumer group ID
		configs.put("group.id", "first-group");
		// PartitionAssignor decides which partitions will be assigned to which consumer
		// By default, Kafka has two assignment strategies:

		// 1) Range : Assigns to each consumer a consecutive subset of partitions from
		// each topic it subscribes to
		// *** C1 and C2, two topics T1 & T2 each of wish has 3 partitions
		// ==> C1:part0,part1 from T1 & C2 part2 from T1, part2 from T2
		// ==> because the assignment is done for each topic independently, the first
		// consumer ends up with more partitions than the second

		// 2) RoundRobin : assigns partitions to consumers sequentially, one by one.
		// *** C1 and C2, two topics T1 & T2 each of wish has 3 partitions
		// ==> C1 would have partitions 0 and 2 from topic T1 and partition 1 from topic
		// T2. C2 would have partition 1 from topic T1 and partitions 0 and 2 from topic
		// T2
		// ==> RoundRobin assignment will end up with all consumers having the same
		// number of partitions (or at most 1 partition difference)
		configs.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RoundRobinAssignor");

		// Create kafka consumer instance
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
		consumer.subscribe(Collections.singleton("koukitest"));
		// we can use regular expression
		// consumer.subscribe(Collections.singleton("kouki.*"));
		try {
			while (true) {

				// Poll method is used to retrieving data
				// Polling kafka is very important, without it the consumer will be considered
				// as dead
				// The parameter passed to poll method is used to :
				// *** If there is available data ==> consumer will read it immediately
				// *** otherwise, it will wait for the specified number of milliseconds for data
				// to arrive from the broker
				// poll() returns a list of records
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
				for (ConsumerRecord<String, String> record : records) {
					System.out.println("topic = " + record.topic() + ";offset = " + record.offset() + ";key = "
							+ record.key() + ";value = " + record.value());
				}
			}
		} finally {
			// It will also trigger a rebalance immediately rather than wait for the group
			// coordinator to discover that the consumer stopped sending heartbeats
			// and is likely dead
			consumer.close();
		}
	}
}

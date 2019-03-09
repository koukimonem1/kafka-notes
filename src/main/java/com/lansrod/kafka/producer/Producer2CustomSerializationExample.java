package com.lansrod.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

//This example is pretty simple, but you can see how
//fragile the code is. If we ever have too many customers, for example, and need to
//change customerID to Long , or if we ever decide to add a startDate field to Cus-
//tomer , we will have a serious issue in maintaining compatibility between old and new
//messages. Debugging compatibility issues between different versions of serializers
//and deserializers is fairly challenging—you need to compare arrays of raw bytes. To
//make matters even worse, if multiple teams in the same company end up writing Cus-
//tomer data to Kafka, they will all need to use the same serializers and modify the code
//at the exact same time.

public class Producer2CustomSerializationExample {

	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "com.lansrod.kafka.CustomerSerializer");
		KafkaProducer<String, Customer> producer = new KafkaProducer<>(properties);
		ProducerRecord<String, Customer> record1 = new ProducerRecord<>("customer",
				(new Producer2CustomSerializationExample()).new Customer(1, "kouki test"));
		ProducerRecord<String, Customer> record2 = new ProducerRecord<>("customer",
				(new Producer2CustomSerializationExample()).new Customer(1, "custom serializer example"));
		try {
			producer.send(record1).get();
			producer.send(record2).get();
		} catch (Exception e) {
			e.printStackTrace();
		}
		producer.close();
	}

	public class Customer {
		private int customerID;
		private String customerName;

		public Customer(int ID, String name) {
			this.customerID = ID;
			this.customerName = name;
		}

		public int getID() {
			return customerID;
		}

		public String getName() {
			return customerName;
		}
	}
}

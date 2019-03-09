package com.lansrod.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.lansrod.kafka.producer.Producer2CustomSerializationExample.Customer;

//The schema is usually described in JSON and the serialization is usually to binary files, although serializing
//to JSON is also supported. Avro assumes that the schema is present when reading and
//writing files, usually by embedding the schema in the files themselves.

//***********************************************************************************

//One of the most interesting features of Avro, and what makes it a good fit for use in a
//messaging system like Kafka, is that when the application that is writing messages
//switches to a new schema, the applications reading the data can continue processing
//messages without requiring any change or update.

public class Producer3SerialisationUsingAvro {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
		props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
		// props.put("schema.registry.url", schemaUrl);
		String topic = "customerContacts";
		Producer<String, Customer> producer = new KafkaProducer<>(props);

		Customer customer = (new Producer2CustomSerializationExample()).new Customer(7, "kouki");
		System.out.println("Generated customer " + customer.toString());
		ProducerRecord<String, Customer> record = new ProducerRecord<>(topic, String.valueOf(customer.getID()),
				customer);
		producer.send(record);
		
		producer.close(); 
	}

}

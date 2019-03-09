package com.lansrod.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Producer1Example {

	public static void main(String[] args) {
		Properties properties = new Properties();

		// Mandatory configurations
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

		// Optional configurations

		/**** aks ****/
//		If acks=0 , the producer will not wait for a reply from the broker before assuming
//				the message was sent successfully.
//		If acks=1 , the producer will receive a success response from the broker the
//				moment the leader replica received the message
//		If acks=all , the producer will receive a success response from the broker once all
//				in-sync replicas received the message.
		
		properties.put("acks", 1);
		
		/**** buffer.memory ****/
//		This sets the amount of memory the producer will use to buffer messages waiting to
//		be sent to brokers.
		
		properties.put("buffer.memory", 33554432);
		
		/***** compression.type ****/
		
		/***** retries ****/
		
		properties.put("buffer.memory", 33554432);
		// **** batch.size ****//
		// **** linger.ms ****//
		// **** client.id ****//

//	***********************	Fire-and-forget *******************
//		We send a message to the server and don’t really care if it arrives succesfully or
//		not. Most of the time, it will arrive successfully, since Kafka is highly available
//		and the producer will retry sending messages automatically. However, some mes‐
//		sages will get lost using this method.
		ProducerRecord<String, String> record = new ProducerRecord<>("javaapitest", "attemp", "java api");
		try {
			producer.send(record);
		} catch (Exception e) {
			// but we still expect the errors that occur in the producer itself (and not if
			// the messages are well sent or not !!!)
			// e.g: SerializationException, BufferExhaus BufferExhaustedException or
			// TimeoutException
			e.printStackTrace();
		}

//	*********************	Synchronous send ***********************
//		We send a message, the send() method returns a Future object, and we use get()
//		to wait on the future and see if the send() was successful or not.
		ProducerRecord<String, String> record2 = new ProducerRecord<>("javaapitest", "second attemp",
				"synchrounised message");
		try {
//			Here, we are using Future.get() to wait for a reply from Kafka. This method
//			will throw an exception if the record is not sent successfully to Kafka.
			producer.send(record2).get();
		} catch (Exception e) {
			e.printStackTrace();
		}

//		 ********************** Asynchronous send **********************
//		We call the send() method with a callback function, which gets triggered when it
//		receives a response from the Kafka broker.
		/**
		 * Suppose the network roundtrip time between our application and the Kafka
		 * cluster is 10ms. If we wait for a reply after sending each message, sending
		 * 100 messages will take around 1 second. On the other hand, if we just send
		 * all our messages and not wait for any replies. In order to send messages
		 * asynchronously and still handle error scenarios, the pro‐ ducer supports
		 * adding a callback when sending a record.
		 */

		ProducerRecord<String, String> record3 = new ProducerRecord<>("javaapitest", "third attempt",
				"asynchrounised message 1");
		ProducerRecord<String, String> record4 = new ProducerRecord<>("javaapitest", "third attempt",
				"asynchrounised message 2");
		ProducerRecord<String, String> record5 = new ProducerRecord<>("javaapitest", "third attempt",
				"asynchrounised message 3");
		ProducerRecord<String, String> record6 = new ProducerRecord<>("javaapitest", "third attempt",
				"asynchrounised message 4");
		producer.send(record3, (new Producer1Example()).new CallbackExample());
		producer.send(record4, (new Producer1Example()).new CallbackExample());
		producer.send(record5, (new Producer1Example()).new CallbackExample());
		producer.send(record6, (new Producer1Example()).new CallbackExample());
		producer.close();
	}

	private class CallbackExample implements Callback {

		@Override
		public void onCompletion(RecordMetadata metadata, Exception exception) {
			if (exception != null)
				exception.printStackTrace();
		}

	}

}

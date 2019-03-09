package com.lansrod.kafka.producer;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import com.lansrod.kafka.producer.Producer2CustomSerializationExample.Customer;

public class Producer2CustomerSerializer implements Serializer<Customer> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub

	}

	@Override
	public byte[] serialize(String topic, Customer customer) {
		byte[] custmerName;
		int customerNameSize;
		try {
			if (customer == null)
				return null;
			else {
				if (customer.getName() != null) {
					custmerName = customer.getName().getBytes("UTF-8");
					customerNameSize = custmerName.length;
				} else {
					custmerName = new byte[0];
					customerNameSize = 0;
				}
			}
			ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + customerNameSize);
			buffer.putInt(customer.getID());
			buffer.putInt(customerNameSize);
			buffer.put(custmerName);
			return buffer.array();
		} catch (Exception e) {
			throw new SerializationException("Error when serializing Customer to byte[] " + e);
		}
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
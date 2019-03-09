package com.lansrod.kafka.producer;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;

public class Producer4CustomPartitioner implements Partitioner {

	@Override
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub

	}
	
	// Implement the logic for the partitioner
	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
		// this return the number of partitions for a topic
		int numPartitions = partitions.size();
		if ((keyBytes == null) || (!(key instanceof String)))
			throw new InvalidRecordException("We expect all messages to have customer name as key");
		if (((String) key).equals("part 1"))
			return 1;
		else if (((String) key).equals("part 2"))
			return 2;
		else
			return 3;

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}

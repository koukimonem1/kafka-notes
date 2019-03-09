package com.lansrod.kafka.consumer;

//***** If number of consumer > number of partitions ==> some of the consumers will be idle and get no messages at all

//*******************************************************************************
//******************** PARTITION REBALANCE ***************************************
//*******************************************************************************

//Moving partition ownership from one consumer to another is called a rebalance.
//## When we add a new consumer to the group, it starts consuming messages from partitions previously consumed by another consumer.
//## The same thing happens when a consumer shuts down or crashes; it leaves the group, and the partitions it used to consume will be consumed by one of the remaining consumers
//## Reassignment of partitions to consumers also happen when we add new partitions

//During a rebalance, consumers can’t consume messages ==> rebalance is a short window of  unavailability of the entire consumer group. 
//when partitions are moved from one consumer to another, the consumer loses its current state; if it was caching any data
// ===> it will need to refresh its caches—slowing down the application until the consumer sets up its state again.

// #### GROUP COORDINATOR: consumers maintain membership in a consumer group and ownership of
//the partitions assigned to them is by sending heartbeats to a Kafka broker designated
//as the group coordinator (this broker can be different for different consumer groups)
//#### Heartbeats are sent when the consumer polls (i.e., retrieves records) and when it commits records it has consumed
//#### If a consumer crashed and stopped processing messages, it will take the group coordinator a few seconds without heartbeats to decide it is dead and trigger the rebalance
//#### During those seconds, no messages will be processed from the partitions owned by the dead consumer

//////////////////////////////////////////How Does the Process of Assigning Partitions to Brokers Work? //////////////////////////////

// 1) When a consumer wants to join a group, it sends a JoinGroup request to the group coordinator.
// 2) The first consumer to join the group becomes the group leader.
// 3) The leader receives a list of all consumers in the group from the group coordinator (this will include all consumers 
// that sent a heartbeat recently and which are therefore considered alive) and is responsible for assigning a subset of partitions to each consumer(look at (4) ). 
// 4) It uses an implementation of PartitionAssignor to decide which partitions should be handled by which consumer.
// 5) The consumer leader sends the list of assignments to the GroupCoordinator
// 6) Each consumer only sees his own assignment—the leader is the only client process that has the full list of consumers in the group and their assignments
// 7) This process repeats every time a rebalance happens
public class Consumer1Notes {

}

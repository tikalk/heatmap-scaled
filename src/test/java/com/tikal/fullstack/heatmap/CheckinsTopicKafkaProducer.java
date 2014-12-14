package com.tikal.fullstack.heatmap;

import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class CheckinsTopicKafkaProducer {
	private static final String sampleAddress = "@ADDRESS:1600+Amphitheatre+Parkway,+Mountain+View,+CA";
	private static final String serClass = "kafka.serializer.StringEncoder";
	private static final String zkHosts = "localhost:9092";
	private static final String topicName = "checkinsTopic";
	
	public static void main(final String[] args) throws InterruptedException {
		final Producer<String, String> producer = createProducer();
		
		for(;true;Thread.sleep(1000))
            producer.send(new KeyedMessage<String, String>(topicName,new Date().getTime()+sampleAddress));        

//		producer.close();
	}

	private static Producer<String, String> createProducer() {
		final Properties props = new Properties();
		props.put("metadata.broker.list", zkHosts);
		props.put("serializer.class", serClass);
		final ProducerConfig config = new ProducerConfig(props);
		final Producer<String, String> producer = new Producer<>(config);
		return producer;
	}
	

}

package com.tikal.fullstack.heatmap;

import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.junit.Test;

public class CheckinsTopicKafkaProducerTest {
	private final String topicName = "checkinsTopic";
	@Test
	public void testCheckins() throws InterruptedException {
		final Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		final ProducerConfig config = new ProducerConfig(props);
		final Producer<String, String> producer = new Producer<String, String>(config);
		
		while(true){
            producer.send(new KeyedMessage<String, String>(topicName,new Date().getTime()+"@ADDRESS:1600+Amphitheatre+Parkway,+Mountain+View,+CA"));
            Thread.sleep(1000);
            
		}

//		producer.close();
	}
	

}

package com.tikal.fullstack.heatmap.topology.bolts;

import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class KafkaOutputBolt extends BaseRichBolt{

	private Producer<String, String> producer;
	private String metadataBrokerList, serializerClass, topic;
	
	public KafkaOutputBolt(String metadataBrokerList, String serializerClass, String topic) {
		this.metadataBrokerList = metadataBrokerList;
		this.serializerClass = serializerClass;
		this.topic = topic;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,OutputCollector collector) {
		Properties props = new Properties();
		props.put("metadata.broker.list", metadataBrokerList);
		props.put("serializer.class", serializerClass);
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
		
	}

	@Override
	public void execute(Tuple tuple) {
		String dbKey = tuple.getStringByField("dbKey");
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, dbKey);
		producer.send(data);
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
	
}
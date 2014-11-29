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
	private final String metadataBrokerList, serializerClass, topic;
	
	public KafkaOutputBolt(final String metadataBrokerList, final String serializerClass, final String topic) {
		this.metadataBrokerList = metadataBrokerList;
		this.serializerClass = serializerClass;
		this.topic = topic;
	}

	@Override
	public void prepare(final Map stormConf, final TopologyContext context,final OutputCollector collector) {
		final Properties props = new Properties();
		props.put("metadata.broker.list", metadataBrokerList);
		props.put("serializer.class", serializerClass);
		final ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
		
	}

	@Override
	public void execute(final Tuple tuple) {
		final String dbKey = tuple.getStringByField("dbKey");
		final String locationsList = tuple.getStringByField("locationsList");
		final KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, dbKey,locationsList);
		producer.send(data);
		
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		
	}
	
}
package com.tikal.fullstack.heatmap.topology;

import java.io.IOException;
import java.util.Properties;

import org.codehaus.jackson.map.ObjectMapper;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.TupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;
import storm.kafka.trident.TridentKafkaState;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.tikal.fullstack.heatmap.topology.bolts.GeocodeLookupBolt;
import com.tikal.fullstack.heatmap.topology.bolts.HeatMapBuilderBolt;
import com.tikal.fullstack.heatmap.topology.locatorservice.impl.MockLocatorService;


public class WorkshopTopologyRunner {
	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WorkshopTopologyRunner.class);
	private static ObjectMapper objectMapper = new ObjectMapper();
	public static void main(final String[] args) {
		final int intervalWindow = 2;
		final int emitInterval = 3;
		
		final String zkHosts = "localhost";
		final String kafkaTopicName="checkinsTopic";

		final TopologyBuilder builder = buildTopolgy(kafkaTopicName,zkHosts,intervalWindow, emitInterval);


		final Config config = new Config();
		config.setDebug(false);
		// config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);

		final LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("local-heatmap", config, builder.createTopology());
		logger.info("##############Topology submitted################");
		
//		Config conf = new Config();
//		conf.setNumWorkers(20);
//		conf.setMaxSpoutPending(5000);
//		StormSubmitter.submitTopology("mytopology", conf, builder.createTopology());

	}

	private static TopologyBuilder buildTopolgy(final String kafkaTopicName,final String zkHosts,final int intervalWindow, final int emitInterval) {
		
		final SpoutConfig kafkaConfig = getSoutConfig(kafkaTopicName, zkHosts);
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		final Config heatmapConfig = new Config();
		heatmapConfig.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitInterval);
		heatmapConfig.put("interval-window", intervalWindow);

		
		final TopologyBuilder builder = new TopologyBuilder();

//		builder.setSpout("checkins", new Checkins());
		builder.setSpout("checkins", new KafkaSpout(kafkaConfig), 4);

		builder.setBolt("geocode-lookup", new GeocodeLookupBolt(), 8).setNumTasks(64).shuffleGrouping("checkins").addConfiguration("locatorService", new MockLocatorService().getClass().getName());
		builder.setBolt("heatmap-builder", new HeatMapBuilderBolt(), 4).fieldsGrouping("geocode-lookup",new Fields("city")).addConfigurations(heatmapConfig );
//		builder.setBolt("persistor", new PersistorBolt(), 2).setNumTasks(4).shuffleGrouping("heatmap-builder");
//		builder.setBolt("kafkaProducer", new KafkaOutputBolt("localhost:9092","kafka.serializer.StringEncoder","locations-topic"), 2).setNumTasks(4).shuffleGrouping("persistor");
		builder.setBolt("kafkaProducer",getKafkaBolt(),2).setNumTasks(4).shuffleGrouping("heatmap-builder").addConfigurations(getKafkaBoltConfig());
		

		return builder;
	}

	private static KafkaBolt<String, String> getKafkaBolt() {
		return new KafkaBolt<String,String>()
		.withTopicSelector(new DefaultTopicSelector("locations-topic"))
		.withTupleToKafkaMapper(new TupleToKafkaMapper<String, String>() {			
			@Override
			public String getMessageFromTuple(final Tuple tuple) {
				try {
					return objectMapper.writeValueAsString(tuple.getValueByField("locationsList"));
				} catch (final IOException e) {
					throw new RuntimeException(e);
				}
			}			
			@Override
			public String getKeyFromTuple(final Tuple tuple) {
				return "checkins-" +tuple.getLongByField("time-interval")+"@"+tuple.getStringByField("city");				
			}
		});
	}
	
	

	private static SpoutConfig getSoutConfig(final String kafkaTopicName, final String zkHosts) {
		final SpoutConfig kafkaConfig = new SpoutConfig(new ZkHosts(zkHosts), kafkaTopicName, "", "storm");
		return kafkaConfig;
	}
	
	
	private static Config getKafkaBoltConfig() {
		final Config config = new Config();
		final Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("request.required.acks", "1");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        config.put(TridentKafkaState.KAFKA_BROKER_PROPERTIES, props);
        return config;
    }
}
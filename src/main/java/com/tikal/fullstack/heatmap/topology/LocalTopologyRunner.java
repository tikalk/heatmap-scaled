package com.tikal.fullstack.heatmap.topology;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.tikal.fullstack.heatmap.topology.bolts.GeocodeLookupBolt;
import com.tikal.fullstack.heatmap.topology.bolts.HeatMapBuilderBolt;
import com.tikal.fullstack.heatmap.topology.bolts.KafkaOutputBolt;
import com.tikal.fullstack.heatmap.topology.bolts.PersistorBolt;

public class LocalTopologyRunner {
	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LocalTopologyRunner.class);
	public static void main(final String[] args) {
		final int intervalWindow = 2;
		final int emitInterval = 3;
		
		final String solrAddress = "http://localhost:8080";
		final String zkHosts = "localhost";
		final String kafkaTopicName="checkinsTopic";

		final TopologyBuilder builder = buildTopolgy(kafkaTopicName,zkHosts,intervalWindow, emitInterval, solrAddress);


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

	private static TopologyBuilder buildTopolgy(final String kafkaTopicName,final String zkHosts,final int intervalWindow, final int emitInterval, final String solrAddress) {
		
		final SpoutConfig kafkaConfig = new SpoutConfig(new ZkHosts(zkHosts), kafkaTopicName, "", "storm");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		final Config heatmapConfig = new Config();
		heatmapConfig.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitInterval);
		heatmapConfig.put("interval-window", intervalWindow);
		
		final TopologyBuilder builder = new TopologyBuilder();

//		builder.setSpout("checkins", new Checkins());
		builder.setSpout("checkins", new KafkaSpout(kafkaConfig), 4);

		builder.setBolt("geocode-lookup", new GeocodeLookupBolt(), 8).setNumTasks(64).shuffleGrouping("checkins");
		builder.setBolt("heatmap-builder", new HeatMapBuilderBolt(), 4).fieldsGrouping("geocode-lookup",new Fields("city")).addConfigurations(heatmapConfig );
		builder.setBolt("persistor", new PersistorBolt(), 2).setNumTasks(4).shuffleGrouping("heatmap-builder");
		builder.setBolt("kafkaProducer", new KafkaOutputBolt("localhost:9092","kafka.serializer.StringEncoder","locations-topic"), 2).setNumTasks(4).shuffleGrouping("persistor");
		
//		builder.setBolt("indexer", new IndexerBolt(solrAddress), 1).setNumTasks(4).shuffleGrouping("persistor");
		return builder;
	}
}
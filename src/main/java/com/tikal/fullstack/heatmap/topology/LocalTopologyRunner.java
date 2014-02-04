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
import com.tikal.fullstack.heatmap.topology.bolts.IndexerBolt;
import com.tikal.fullstack.heatmap.topology.bolts.KafkaOutputBolt;
import com.tikal.fullstack.heatmap.topology.bolts.PersistorBolt;

public class LocalTopologyRunner {
	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LocalTopologyRunner.class);
	public static void main(String[] args) {
		int intervalWindow = 2;
		int emitInterval = 3;
		
		String solrAddress = "http://localhost:8080";
		String zkHosts = "localhost";
		String kafkaTopicName="checkinsTopic";

		TopologyBuilder builder = buildTopolgy(kafkaTopicName,zkHosts,intervalWindow, emitInterval, solrAddress);


		Config config = new Config();
		config.setDebug(false);
		// config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);

		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("local-heatmap", config, builder.createTopology());
		logger.info("##############Topology submitted################");
		
//		Config conf = new Config();
//		conf.setNumWorkers(20);
//		conf.setMaxSpoutPending(5000);
//		StormSubmitter.submitTopology("mytopology", conf, builder.createTopology());

	}

	private static TopologyBuilder buildTopolgy(String kafkaTopicName,String zkHosts,int intervalWindow, int emitInterval, String solrAddress) {
		
		SpoutConfig kafkaConfig = new SpoutConfig(new ZkHosts(zkHosts), kafkaTopicName, "", "storm");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		Config heatmapConfig = new Config();
		heatmapConfig.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitInterval);
		heatmapConfig.put("interval-window", intervalWindow);
		
		TopologyBuilder builder = new TopologyBuilder();

//		builder.setSpout("checkins", new Checkins());
		builder.setSpout("checkins", new KafkaSpout(kafkaConfig), 4);

		builder.setBolt("geocode-lookup", new GeocodeLookupBolt(), 8).setNumTasks(64).shuffleGrouping("checkins");
		builder.setBolt("heatmap-builder", new HeatMapBuilderBolt(), 4).fieldsGrouping("geocode-lookup",
				new Fields("city")).addConfigurations(heatmapConfig );
		builder.setBolt("persistor", new PersistorBolt(), 2).setNumTasks(4).shuffleGrouping("heatmap-builder");
		 builder.setBolt("kafkaProducer", new KafkaOutputBolt("localhost:9092","kafka.serializer.StringEncoder","locations-topic"), 2).setNumTasks(4).shuffleGrouping("persistor");
		
		builder.setBolt("indexer", new IndexerBolt(solrAddress), 1).setNumTasks(4).shuffleGrouping("persistor");
		return builder;
	}
}
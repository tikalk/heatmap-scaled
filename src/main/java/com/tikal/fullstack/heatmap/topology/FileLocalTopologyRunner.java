package com.tikal.fullstack.heatmap.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.tikal.fullstack.heatmap.topology.bolts.GeocodeLookupBolt;
import com.tikal.fullstack.heatmap.topology.bolts.HeatMapBuilderBolt;
import com.tikal.fullstack.heatmap.topology.bolts.PersistorBolt;
import com.tikal.fullstack.heatmap.topology.spouts.CheckinsSpout;

public class FileLocalTopologyRunner {
	public static void main(String[] args) {
		int intervalWindow = 2;
		int emitInterval = 3;

		TopologyBuilder builder = buildTopolgy(intervalWindow, emitInterval);


		Config config = new Config();
		config.setDebug(false);

		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("local-heatmap", config, builder.createTopology());
	}

	private static TopologyBuilder buildTopolgy(int intervalWindow, int emitInterval) {
		
		
		Config heatmapConfig = new Config();
		heatmapConfig.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitInterval);
		heatmapConfig.put("interval-window", intervalWindow);
		
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("checkins", new CheckinsSpout());
		

		builder.setBolt("geocode-lookup", new GeocodeLookupBolt(), 8).setNumTasks(64).shuffleGrouping("checkins");
		builder.setBolt("heatmap-builder", new HeatMapBuilderBolt(), 4).fieldsGrouping("geocode-lookup",
				new Fields("city")).addConfigurations(heatmapConfig );
		builder.setBolt("persistor", new PersistorBolt(), 2).setNumTasks(4).shuffleGrouping("heatmap-builder");
		
		return builder;
	}
}
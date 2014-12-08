package com.tikal.fullstack.heatmap.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.tikal.fullstack.heatmap.topology.bolts.GeocodeLookupBolt;
import com.tikal.fullstack.heatmap.topology.bolts.HeatMapBuilderBolt;
import com.tikal.fullstack.heatmap.topology.bolts.PersistorBolt;
import com.tikal.fullstack.heatmap.topology.locatorservice.impl.MockLocatorService;
import com.tikal.fullstack.heatmap.topology.locatorservice.impl.RedisLocatorService;
import com.tikal.fullstack.heatmap.topology.spouts.CheckinsSpout;

public class FileLocalTopologyRunner {
	public static void main(final String[] args) {
		final int intervalWindow = 2;
		final int emitInterval = 3;

		final TopologyBuilder builder = buildTopolgy(intervalWindow, emitInterval);
		


		final Config config = new Config();
		config.setDebug(false);

		final LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("local-heatmap", config, builder.createTopology());
	}

	private static TopologyBuilder buildTopolgy(final int intervalWindow, final int emitInterval) {
		
		
		final Config heatmapConfig = new Config();
		heatmapConfig.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitInterval);
		heatmapConfig.put("interval-window", intervalWindow);
		
		final TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("checkins", new CheckinsSpout());
		

		builder.setBolt("geocode-lookup", new GeocodeLookupBolt(), 8).setNumTasks(64).shuffleGrouping("checkins").addConfiguration("locatorService", new RedisLocatorService().getClass().getName());
		builder.setBolt("heatmap-builder", new HeatMapBuilderBolt(), 4).fieldsGrouping("geocode-lookup",
				new Fields("city")).addConfigurations(heatmapConfig );
		builder.setBolt("persistor", new PersistorBolt(), 2).setNumTasks(4).shuffleGrouping("heatmap-builder");
		
		return builder;
	}
}
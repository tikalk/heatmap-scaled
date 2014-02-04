package com.tikal.fullstack.heatmap.topology.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class CheckinsSpout extends BaseRichSpout {
	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CheckinsSpout.class);
	private List<String> sampleLocations;
	private int nextEmitIndex;
	private SpoutOutputCollector outputCollector;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("str"));
	}

	@Override
	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		this.outputCollector = spoutOutputCollector;
		this.nextEmitIndex = 0;

		try {
			sampleLocations = IOUtils.readLines(ClassLoader.getSystemResourceAsStream("sample-locations.txt"), Charset
					.defaultCharset().name());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void nextTuple() {
		String address = sampleLocations.get(nextEmitIndex);
		String addressStr = new Date().getTime()+"@ADDRESS:"+address;
		Values tuple = new Values(addressStr);
		logger.debug("Emitting:"+tuple);
		outputCollector.emit(tuple);

		nextEmitIndex = (nextEmitIndex + 1) % sampleLocations.size();

		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

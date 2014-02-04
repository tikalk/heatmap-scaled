package com.tikal.fullstack.heatmap.topology.bolts;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.tikal.fullstack.heatmap.topology.dto.LocationDTO;
import com.tikal.fullstack.heatmap.topology.locatorservice.LocatorService;
import com.tikal.fullstack.heatmap.topology.locatorservice.impl.RedisLocatorService;

public class GeocodeLookupBolt extends BaseBasicBolt {
	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GeocodeLookupBolt.class);
	private LocatorService locatorService;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer fieldsDeclarer) {
		fieldsDeclarer.declare(new Fields("city","time", "location"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
//		locatorService = new GoogleLocatorService();
		locatorService = new RedisLocatorService();
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
		logger.debug("Got:"+tuple);
		String str = tuple.getStringByField("str");
		String[] parts = str.split("@");
		Long time = Long.valueOf(parts[0]);
		String address = parts[1];
		
		LocationDTO locationDTO = locatorService.getLocation(address);
		if(locationDTO!=null) {
			Values emitTuple = new Values(locationDTO.getCity(),time,locationDTO);
			logger.debug("Emit:"+emitTuple);
			outputCollector.emit(emitTuple );
		}
	}

}

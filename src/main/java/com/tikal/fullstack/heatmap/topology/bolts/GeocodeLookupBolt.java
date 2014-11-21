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
	public void declareOutputFields(final OutputFieldsDeclarer fieldsDeclarer) {
		fieldsDeclarer.declare(new Fields("city","time", "location"));
	}

	@Override
	public void prepare(final Map stormConf, final TopologyContext context) {
//		locatorService = new GoogleLocatorService();
		locatorService = new RedisLocatorService();
	}

	@Override
	public void execute(final Tuple tuple, final BasicOutputCollector outputCollector) {
		logger.debug("Got:"+tuple);
		final String str = tuple.getStringByField("str");
		final String[] parts = str.split("@");
		final Long time;
		try{
			time = Long.valueOf(parts[0]);
		} catch(final NumberFormatException ex){
			logger.error(parts[0] +"-"+ex.getMessage());
			return;
		}
		final String address = parts[1];
		
		final LocationDTO locationDTO = locatorService.getLocation(address);
		if(locationDTO!=null) {
			final Values emitTuple = new Values(locationDTO.getCity(),time,locationDTO);
			logger.debug("Emit:"+emitTuple);
			outputCollector.emit(emitTuple );
		}
	}

}

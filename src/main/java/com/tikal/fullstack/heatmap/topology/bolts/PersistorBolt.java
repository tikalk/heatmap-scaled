package com.tikal.fullstack.heatmap.topology.bolts;

import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.tikal.fullstack.heatmap.topology.dto.LocationDTO;

public class PersistorBolt extends BaseBasicBolt {
	private final Logger logger = LoggerFactory.getLogger(PersistorBolt.class);

	private Jedis jedis;
	private ObjectMapper objectMapper;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		jedis = new Jedis("localhost");
		objectMapper = new ObjectMapper();
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
		Long timeInterval = tuple.getLongByField("time-interval");
		String city = tuple.getStringByField("city");
		List<LocationDTO> locationsList = (List<LocationDTO>) tuple.getValueByField("locationsList");
		try {
			String dbKey = "checkins-" + timeInterval+"@"+city;
			String value = objectMapper.writeValueAsString(locationsList);
			logger.info("*****************************"+dbKey+" : "+value);
			jedis.set(dbKey, value);
			jedis.publish("location-key", dbKey);
			outputCollector.emit(new Values(dbKey,locationsList));
		} catch (Exception e) {
			logger.error("Error persisting for time: " + timeInterval, e);
		}
	}


	@Override
	public void cleanup() {
		jedis.quit();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("dbKey","locationsList"));
	}
}

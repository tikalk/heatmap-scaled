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
	public void prepare(final Map stormConf, final TopologyContext context) {
		jedis = new Jedis("localhost");
		objectMapper = new ObjectMapper();
	}

	@Override
	public void execute(final Tuple tuple, final BasicOutputCollector outputCollector) {
		final Long timeInterval = tuple.getLongByField("time-interval");
		final String city = tuple.getStringByField("city");
		final List<LocationDTO> locationsList = (List<LocationDTO>) tuple.getValueByField("locationsList");
		try {
			final String dbKey = "checkins-" + timeInterval+"@"+city;
			final String value = objectMapper.writeValueAsString(locationsList);
			logger.info("*****************************"+dbKey+" : "+value);
			jedis.set(dbKey, value);
			jedis.publish("location-key", dbKey);
			outputCollector.emit(new Values(dbKey,locationsList.toString()));
		} catch (final Exception e) {
			logger.error("Error persisting for time: " + timeInterval, e);
		}
	}


	@Override
	public void cleanup() {
		jedis.quit();
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("dbKey","locationsList"));
	}
}

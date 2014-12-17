package com.tikal.fullstack.heatmap.topology.bolts;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.tikal.fullstack.heatmap.topology.dto.LocationDTO;

public class HeatMapBuilderBolt extends BaseBasicBolt {
	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HeatMapBuilderBolt.class);
	private Map<String, List<LocationDTO>> heatmaps;
	
	private long intervalWindow;

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time-interval", "city","locationsList"));
	}

	@Override
	public void prepare(final Map stormConf, final TopologyContext context) {
		heatmaps = new HashMap<String, List<LocationDTO>>();
		intervalWindow = (long) stormConf.get("interval-window");
	}


	@Override
	public void execute(final Tuple tuple, final BasicOutputCollector outputCollector) {
		if (isTickTuple(tuple)) {
			emitHeatmap(outputCollector);
		} else {
			final Long time = tuple.getLongByField("time");
			final LocationDTO locationDTO = (LocationDTO) tuple.getValueByField("location");
			
			final Long timeInterval = selectTimeInterval(time);			
			final List<LocationDTO> locationsList = getCheckinsForInterval(timeInterval,locationDTO.getCity());
			locationsList.add(locationDTO);
		}
	}

	private boolean isTickTuple(final Tuple tuple) {
		final String sourceComponent = tuple.getSourceComponent();
		final String sourceStreamId = tuple.getSourceStreamId();
		return sourceComponent.equals(Constants.SYSTEM_COMPONENT_ID)
				&& sourceStreamId.equals(Constants.SYSTEM_TICK_STREAM_ID);
	}

	private void emitHeatmap(final BasicOutputCollector outputCollector) {
		final Long now = System.currentTimeMillis();
		final Long emitUpToTimeInterval = selectTimeInterval(now);
		final Set<String> timeIntervalsAvailableWithCityToRemove = new HashSet<>();
		final Set<String> timeIntervalsAvailableWithCity = heatmaps.keySet();
		for (final String timeIntervalWithCity : timeIntervalsAvailableWithCity) {
			final String city = timeIntervalWithCity.split("@")[0];
			final long timeInterval = Long.valueOf(timeIntervalWithCity.split("@")[1]);
			if (timeInterval <= emitUpToTimeInterval) {
				timeIntervalsAvailableWithCityToRemove.add(timeIntervalWithCity);
				final List<LocationDTO> hotzones = heatmaps.get(timeIntervalWithCity);
				final Values values = new Values(timeInterval,city, hotzones);
				logger.debug("Emit:"+values);
				outputCollector.emit(values);
			}
		}
		for (final String key : timeIntervalsAvailableWithCityToRemove) 
			heatmaps.remove(key);
	}

	private Long selectTimeInterval(final Long time) {
		return time / (intervalWindow * 1000);
	}

	private List<LocationDTO> getCheckinsForInterval(final Long timeInterval, final String city) {
		List<LocationDTO> hotzones = heatmaps.get(city+"@"+timeInterval);
		if (hotzones == null) {
			hotzones = new ArrayList<LocationDTO>();
			heatmaps.put(city+"@"+timeInterval, hotzones);
		}
		return hotzones;
	}
}

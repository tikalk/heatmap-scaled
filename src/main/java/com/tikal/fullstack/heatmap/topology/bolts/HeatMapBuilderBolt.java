package com.tikal.fullstack.heatmap.topology.bolts;

import java.util.ArrayList;
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
	private Map<String, List<LocationDTO>> heatmaps;
	
	private long intervalWindow;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time-interval", "city","locationsList"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		heatmaps = new HashMap<String, List<LocationDTO>>();
		intervalWindow = (long) stormConf.get("interval-window");
	}


	@Override
	public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
		if (isTickTuple(tuple)) {
			emitHeatmap(outputCollector);
		} else {
			Long time = tuple.getLongByField("time");
			LocationDTO locationDTO = (LocationDTO) tuple.getValueByField("location");
			
			Long timeInterval = selectTimeInterval(time);			
			List<LocationDTO> locationsList = getCheckinsForInterval(timeInterval,locationDTO.getCity());
			locationsList.add(locationDTO);
		}
	}

	private boolean isTickTuple(Tuple tuple) {
		String sourceComponent = tuple.getSourceComponent();
		String sourceStreamId = tuple.getSourceStreamId();
		return sourceComponent.equals(Constants.SYSTEM_COMPONENT_ID)
				&& sourceStreamId.equals(Constants.SYSTEM_TICK_STREAM_ID);
	}

	private void emitHeatmap(BasicOutputCollector outputCollector) {
		Long now = System.currentTimeMillis();
		Long emitUpToTimeInterval = selectTimeInterval(now);
		Set<String> timeIntervalsAvailableWithCityToRemove = new HashSet<>();
		Set<String> timeIntervalsAvailableWithCity = heatmaps.keySet();
		for (String timeIntervalWithCity : timeIntervalsAvailableWithCity) {
			String city = timeIntervalWithCity.split("@")[0];
			long timeInterval = Long.valueOf(timeIntervalWithCity.split("@")[1]);
			if (timeInterval <= emitUpToTimeInterval) {
				timeIntervalsAvailableWithCityToRemove.add(timeIntervalWithCity);
				List<LocationDTO> hotzones = heatmaps.get(timeIntervalWithCity);
				outputCollector.emit(new Values(timeInterval,city, hotzones));
			}
		}
		for (String key : timeIntervalsAvailableWithCityToRemove) 
			heatmaps.remove(key);
	}

	private Long selectTimeInterval(Long time) {
		return time / (intervalWindow * 1000);
	}

	private List<LocationDTO> getCheckinsForInterval(Long timeInterval, String city) {
		List<LocationDTO> hotzones = heatmaps.get(timeInterval);
		if (hotzones == null) 
			heatmaps.put(city+"@"+timeInterval, new ArrayList<LocationDTO>());
		return hotzones;
	}
}

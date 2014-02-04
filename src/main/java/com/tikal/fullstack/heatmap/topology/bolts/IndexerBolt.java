package com.tikal.fullstack.heatmap.topology.bolts;

import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.codehaus.jackson.map.ObjectMapper;

import com.tikal.fullstack.heatmap.topology.dto.LocationDTO;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class IndexerBolt extends BaseBasicBolt {
	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IndexerBolt.class);
	private SolrServer solrServer;
	private final String solrAddress;
	private ObjectMapper objectMapper;

	public IndexerBolt(String solrAddress) {
		this.solrAddress = solrAddress;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		try {
			objectMapper = new ObjectMapper();
			this.solrServer = new HttpSolrServer(this.solrAddress);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private SolrInputDocument getSolrInputDocumentForInput(Tuple tuple) {
		String dbKey = tuple.getStringByField("dbKey");
		String str = dbKey.substring("checkins-".length());
		String[] split = str.split("@ADDRESS:");
		Long timeInterval = Long.valueOf(split[0]);
		String city = split[1];
		List<LocationDTO> locationsList = (List<LocationDTO>) tuple.getValueByField("locationsList");
		try {			
			SolrInputDocument document = new SolrInputDocument();
			document.addField("id", timeInterval + "@" + city);
			document.addField("timeInterval", timeInterval);
			document.addField("city", city);
			document.addField("checkins", objectMapper.writeValueAsString(locationsList));
			return document;
		} catch (Exception e) {
			logger.error("Error persisting for time: " + timeInterval, e);
			return null;
		}		
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		SolrInputDocument document = getSolrInputDocumentForInput(input);
		if (document != null) {
			try {
				solrServer.add(document);
				solrServer.commit();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

}

package com.tikal.fullstack.heatmap.topology.locatorservice.impl;

import redis.clients.jedis.Jedis;

import com.tikal.fullstack.heatmap.topology.dto.LocationDTO;
import com.tikal.fullstack.heatmap.topology.locatorservice.LocatorService;

public class RedisLocatorService implements LocatorService{
	private Jedis jedis = new Jedis("localhost");

	@Override
	public LocationDTO getLocation(String address) {
		String longLat = jedis.get(address);
		if(longLat==null)
			return null;
		String city = extractCity(address);
		String[] split = longLat.split(",");
		return new LocationDTO(Float.valueOf(split[0]), Float.valueOf(split[1]), address,city);
	}

	private String extractCity(String address) {
		String[] split = address.split(",");
		return split[0]+","+split[1]+","+split[2];
	}

}

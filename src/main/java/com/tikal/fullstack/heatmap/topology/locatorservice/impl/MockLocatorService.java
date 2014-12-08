package com.tikal.fullstack.heatmap.topology.locatorservice.impl;

import com.tikal.fullstack.heatmap.topology.dto.LocationDTO;
import com.tikal.fullstack.heatmap.topology.locatorservice.LocatorService;

public class MockLocatorService implements LocatorService{

	@Override
	public LocationDTO getLocation(final String address) {
		return new LocationDTO(0.1f, 0.2f, address,"some city");
	}

	

}

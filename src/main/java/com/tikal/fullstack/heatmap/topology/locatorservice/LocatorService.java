package com.tikal.fullstack.heatmap.topology.locatorservice;

import com.tikal.fullstack.heatmap.topology.dto.LocationDTO;

public interface LocatorService {
	LocationDTO getLocation(String address) ;
}

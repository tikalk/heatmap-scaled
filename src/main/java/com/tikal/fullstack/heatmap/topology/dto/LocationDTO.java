package com.tikal.fullstack.heatmap.topology.dto;

import java.io.IOException;

import org.codehaus.jackson.map.ObjectMapper;

public class LocationDTO {
	private static ObjectMapper om= new ObjectMapper();
	private float lat;
	private float lon;
	private String title;
	private String city;
	
	public LocationDTO() {
	}

	public LocationDTO(final float lat, final float lon, final String title,final String city) {
		this.lat = lat;
		this.lon = lon;
		this.title = title;
		this.city = city;
	}

	public float getLat() {
		return lat;
	}

	public void setLat(final float lat) {
		this.lat = lat;
	}

	public float getLon() {
		return lon;
	}

	public void setLon(final float lon) {
		this.lon = lon;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(final String title) {
		this.title = title;
	}

	public String getCity() {
		return city;
	}

	public void setCity(final String city) {
		this.city = city;
	}

	@Override
	public String toString() {
		try {
			return om.writeValueAsString(this);
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	

}

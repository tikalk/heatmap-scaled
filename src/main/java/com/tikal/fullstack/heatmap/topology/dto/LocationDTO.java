package com.tikal.fullstack.heatmap.topology.dto;

public class LocationDTO {
	private float lat;
	private float lon;
	private String title;
	private String city;
	
	public LocationDTO() {
	}

	public LocationDTO(float lat, float lon, String title,String city) {
		this.lat = lat;
		this.lon = lon;
		this.title = title;
		this.city = city;
	}

	public float getLat() {
		return lat;
	}

	public void setLat(float lat) {
		this.lat = lat;
	}

	public float getLon() {
		return lon;
	}

	public void setLon(float lon) {
		this.lon = lon;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	

}

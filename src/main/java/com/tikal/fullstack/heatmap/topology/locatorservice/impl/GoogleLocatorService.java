package com.tikal.fullstack.heatmap.topology.locatorservice.impl;

import com.google.code.geocoder.Geocoder;
import com.google.code.geocoder.GeocoderRequestBuilder;
import com.google.code.geocoder.model.GeocodeResponse;
import com.google.code.geocoder.model.GeocoderAddressComponent;
import com.google.code.geocoder.model.GeocoderRequest;
import com.google.code.geocoder.model.GeocoderResult;
import com.google.code.geocoder.model.GeocoderStatus;
import com.google.code.geocoder.model.LatLng;
import com.tikal.fullstack.heatmap.topology.dto.LocationDTO;
import com.tikal.fullstack.heatmap.topology.locatorservice.LocatorService;

public class GoogleLocatorService implements LocatorService {
	private Geocoder geocoder = new Geocoder();

	@Override
	public LocationDTO getLocation(String address) {
		GeocoderRequest request = new GeocoderRequestBuilder().setAddress(address).setLanguage("en")
				.getGeocoderRequest();
		GeocodeResponse response = geocoder.geocode(request);
		GeocoderStatus status = response.getStatus();
		LocationDTO locationDTO = null;
		if (GeocoderStatus.OK.equals(status)) {
			GeocoderResult firstResult = response.getResults().get(0);
			LatLng latLng = firstResult.getGeometry().getLocation();
			String city = extractCity(firstResult);
			locationDTO = new LocationDTO(latLng.getLat().floatValue(), latLng.getLng().floatValue(), address,city);
		} else {
			System.out.println("&&&&&&&&&&&&   ERROR CODE   &&&&&&&&&&&&&&&&&&&&&&&&");
		}
		return locationDTO;
	}

	private String extractCity(GeocoderResult result) {
		for (GeocoderAddressComponent component : result.getAddressComponents()) {
			if (component.getTypes().contains("locality"))
				return component.getLongName();
		}
		return "";
	}

}

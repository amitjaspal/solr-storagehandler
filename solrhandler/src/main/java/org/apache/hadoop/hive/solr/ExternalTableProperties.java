package org.apache.hadoop.hive.solr;

import java.util.Map;
import java.util.Properties;

public class ExternalTableProperties {

	static final String SOURCE_URL = "solr.server.url";
	static final String SOURCE_QUERY = "solr.query";
	
	public static void configureExternalTableProperties(Properties tableProperties, Map<String,String> jobProperties){
		
		// Set source-url in the jobProperty
		String sourceURL = tableProperties.getProperty(SOURCE_URL);
		System.out.println("Reading table property URL " + sourceURL);
		jobProperties.put(SOURCE_URL, sourceURL);
		
		// Set source-query in the jobProperty
		String sourceQuery = tableProperties.getProperty(SOURCE_QUERY);
		System.out.println("Reading table property query " + sourceQuery);
		jobProperties.put(SOURCE_QUERY, sourceQuery);
		
	}
	
}

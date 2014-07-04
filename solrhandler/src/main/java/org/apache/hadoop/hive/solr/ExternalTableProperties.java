package org.apache.hadoop.hive.solr;

import java.util.Map;
import java.util.Properties;

public class ExternalTableProperties {

	static final String ZOOKEEPER_SERVICE_URL = "solr.zookeeper.service.url";
	static final String COLLECTION_NAME = "solr.collection.name";
	static final String SOURCE_QUERY = "solr.query";
	
	public static void configureExternalTableProperties(Properties tableProperties, Map<String,String> jobProperties){
		
		// Set zookeeper.service.url in the jobProperty
		String zookeeperService = tableProperties.getProperty(ZOOKEEPER_SERVICE_URL);
		System.out.println("Reading table property zookeeper url " + zookeeperService);
		jobProperties.put(ZOOKEEPER_SERVICE_URL, zookeeperService);
		
		// Set collection.name in the jobProperty
		String collectionName = tableProperties.getProperty(COLLECTION_NAME);
		System.out.println("Reading table property collection name " + collectionName);
		jobProperties.put(COLLECTION_NAME, collectionName);
		
	}
	
}

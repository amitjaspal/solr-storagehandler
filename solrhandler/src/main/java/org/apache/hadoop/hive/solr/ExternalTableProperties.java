package org.apache.hadoop.hive.solr;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.plan.TableDesc;


/*
 * ExternalTableProperties is a holder to keep all the
 * properties of the SOLR backed external table which the user.
 * provided while issuing the CREATE EXTERNAL TABLE command
 */

public class ExternalTableProperties {

  private static final Logger LOG = Logger.getLogger(ExternalTableProperties.class.getName());
  static final String ZOOKEEPER_SERVICE_URL = "solr.zookeeper.service.url";
  static final String COLLECTION_NAME = "solr.collection.name";
  static final String SOLR_QUERY = "solr.query";
  protected static List<String> COLUMN_NAMES;

  /*
   * This method initializes properties of the external table and populates
   * the jobProperties map with them so that they can be used throughout the job.
   */
  public static void initialize(Properties tableProperties, Map<String,String> jobProperties,
      TableDesc tableDesc){

    // Set zookeeper.service.url in the jobProperty
    String zookeeperService = tableProperties.getProperty(ZOOKEEPER_SERVICE_URL);
    LOG.debug("Reading table property zookeeper url : " + zookeeperService);
    jobProperties.put(ZOOKEEPER_SERVICE_URL, zookeeperService);

    // Set collection.name in the jobProperty
    String collectionName = tableProperties.getProperty(COLLECTION_NAME);
    LOG.debug("Reading table property collection name : " + collectionName);
    jobProperties.put(COLLECTION_NAME, collectionName);

    // Set SOLR query in the jobProperty
    String query = tableProperties.getProperty(SOLR_QUERY);
    LOG.debug("Reading table property solr query : " + query);
    if(query == null){
      LOG.info("Setting query to *:*");
      query = "*:*";
      tableProperties.setProperty(SOLR_QUERY, "*:*");
    }
    jobProperties.put(SOLR_QUERY, query);

    String colNamesStr = tableDesc.getProperties().getProperty(hive_metastoreConstants.META_TABLE_COLUMNS);
    COLUMN_NAMES = Arrays.asList(colNamesStr.split(","));
  }
}

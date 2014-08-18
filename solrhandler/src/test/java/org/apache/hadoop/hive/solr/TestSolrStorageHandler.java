package org.apache.hadoop.hive.solr;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSolrStorageHandler extends Assert{

  private static final Logger LOG = Logger.getLogger(TestSolrStorageHandler.class.getName());
  private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
  private static final File HIVE_BASE_DIR = new File("target/hive");
  private static final File HIVE_SCRATCH_DIR = new File(HIVE_BASE_DIR + "/scratchdir");
  private static final File HIVE_LOCAL_SCRATCH_DIR = new File(HIVE_BASE_DIR + "/localscratchdir");
  private static final File HIVE_METADB_DIR = new File(HIVE_BASE_DIR + "/metastoredb");
  private static final File HIVE_LOGS_DIR = new File(HIVE_BASE_DIR + "/logs");
  private static final File HIVE_TMP_DIR = new File(HIVE_BASE_DIR + "/tmp");
  private static final File HIVE_WAREHOUSE_DIR = new File(HIVE_BASE_DIR + "/warehouse");
  private static final File HIVE_TESTDATA_DIR = new File(HIVE_BASE_DIR + "/testdata");
  private static final File HIVE_HADOOP_TMP_DIR = new File(HIVE_BASE_DIR + "/hadooptmp");
  private static final int TIMEOUT = 30000;
  private static final int NUM_SHARDS = 1;
  private static final int REPLICATION_FACTOR = 1;
  private static final String[] SOLR_DATA = {"","Lord of the Rings","Borne Trilogy", "Serendipity", "August Rush",
                                              "Jurasic Park", "Titanic"};
  private static final String COLLECTION_NAME = "testSolrCloudCollection";
  private static final String CONFIG_NAME = "solrCloudCollectionConfig";
  private static final String CONFIGS_ZKNODE = "/configs";
  private static MiniSolrCloudCluster miniCluster;
  private static Connection con;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception{
    // Set up a MiniSolrCloudCluster
    String testHome = SolrTestCaseJ4.TEST_HOME();
    miniCluster = new MiniSolrCloudCluster(1, null, new File(testHome, "solr-no-core.xml"),null, null);
    assertNotNull(miniCluster.getZkServer());
    miniCluster.getZkServer().setTheTickTime(5000);
    assertNotNull(miniCluster.getZkServer());

    // create collection
    System.setProperty("solr.tests.mergePolicy", "org.apache.lucene.index.TieredMergePolicy");
    uploadConfigToZk(SolrTestCaseJ4.TEST_HOME() + File.separator + "collection1" + File.separator + "conf", CONFIG_NAME);
    createCollection(COLLECTION_NAME, NUM_SHARDS, REPLICATION_FACTOR, CONFIG_NAME);

    //insert documents into the collection
    insertDocs();

    // Set up the HIVE directory structure
    FileUtils.forceMkdir(HIVE_BASE_DIR);
    FileUtils.forceMkdir(HIVE_SCRATCH_DIR);
    FileUtils.forceMkdir(HIVE_LOCAL_SCRATCH_DIR);
    FileUtils.forceMkdir(HIVE_LOGS_DIR);
    FileUtils.forceMkdir(HIVE_TMP_DIR);
    FileUtils.forceMkdir(HIVE_WAREHOUSE_DIR);
    FileUtils.forceMkdir(HIVE_HADOOP_TMP_DIR);
    FileUtils.forceMkdir(HIVE_TESTDATA_DIR);

    // Set up the HIVE property in the environment
    System.setProperty("tickTime", "5000");
    System.setProperty("hive.metastore.warehouse.dir",  HIVE_WAREHOUSE_DIR.getAbsolutePath());
    System.setProperty("hive.exec.scratchdir",  HIVE_SCRATCH_DIR.getAbsolutePath());
    System.setProperty("hive.exec.local.scratchdir", HIVE_LOCAL_SCRATCH_DIR.getAbsolutePath());
    System.setProperty("hive.metastore.metadb.dir", HIVE_METADB_DIR.getAbsolutePath());
    System.setProperty("test.log.dir", HIVE_LOGS_DIR.getAbsolutePath());
    System.setProperty("hive.querylog.location", HIVE_TMP_DIR.getAbsolutePath());
    System.setProperty("hadoop.tmp.dir", HIVE_HADOOP_TMP_DIR.getAbsolutePath());
    System.setProperty("derby.stream.error.file",HIVE_BASE_DIR.getAbsolutePath() + "/derby.log");
  }

  @Test
  public void testSolrCollection() throws SolrServerException, MalformedURLException, Exception{
    String zkAddress = miniCluster.getZkServer().getZkAddress();
    CloudSolrServer cloudSolrServer = new CloudSolrServer(zkAddress, true);
    cloudSolrServer.setDefaultCollection(COLLECTION_NAME);
    SolrQuery qry = new SolrQuery();
    qry.setQuery("*:*");
    QueryResponse rsp = cloudSolrServer.query(qry);
    assertEquals(SOLR_DATA.length-1, rsp.getResults().getNumFound());
  }

  @Test
  public void testCreateSolrTable() throws SQLException, ClassNotFoundException, Exception{
    String zkAddress = miniCluster.getZkServer().getZkAddress();
    // load the HIVE-JDBC Driver
    Class.forName(driverName);

    // set up the connection to the HIVE Server
    con = DriverManager.getConnection("jdbc:hive2://", "", "");
    assertNotNull("Connection is null", con);
    assertFalse("Connection should not be closed", con.isClosed());
    Statement s = con.createStatement();
    s.executeUpdate("drop table testTable");
    String query = "create external table testTable( id int) "
                 + " stored by 'org.apache.hadoop.hive.solr.SolrStorageHandler' tblproperties('solr.zookeeper.service.url' = '" + zkAddress + "',"
                 + " 'solr.collection.name' = 'testSolrCloudCollection')";
    s.executeUpdate(query);
    s.close();
    con.close();
  }

  @Test
  public void testSelectSolrTable() throws Exception{
    String zkAddress = miniCluster.getZkServer().getZkAddress();
    // load the HIVE-JDBC Driver
    Class.forName(driverName);

    // set up the connection to the HIVE Server
    con = DriverManager.getConnection("jdbc:hive2://", "", "");
    assertNotNull("Connection is null", con);
    assertFalse("Connection should not be closed", con.isClosed());
    Statement s = con.createStatement();
    s.executeUpdate("drop table testTable");
    String query = "create external table testTable( id int, name string) "
                 + " stored by 'org.apache.hadoop.hive.solr.SolrStorageHandler' tblproperties('solr.zookeeper.service.url' = '" + zkAddress + "',"
                 + " 'solr.collection.name' = 'testSolrCloudCollection')";
    s.executeUpdate(query);
    ResultSet r = s.executeQuery("select * from testTable");
    int i = 1;
    while(r.next()){
      ResultSetMetaData metaData = r.getMetaData();
      int count = metaData.getColumnCount(); //number of column

      for (int k = 1; k <= count; k++)
      {
        if(k == 1)
          assertEquals(i, Integer.parseInt(r.getString(metaData.getColumnLabel(k))));
        else
          assertEquals(SOLR_DATA[i], r.getString(metaData.getColumnLabel(k)));
      }
      i++;
    }
    r.close();
    s.close();
    con.close();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (miniCluster != null) {
      miniCluster.shutdown();
    }
    miniCluster = null;
    System.clearProperty("solr.tests.mergePolicy");
    System.clearProperty("solr.tests.maxBufferedDocs");
    System.clearProperty("solr.tests.maxIndexingThreads");
    System.clearProperty("solr.tests.ramBufferSizeMB");
    System.clearProperty("solr.tests.mergeScheduler");
    System.clearProperty("solr.directoryFactory");
    System.clearProperty("solr.solrxml.location");
    System.clearProperty("zkHost");
  }

  protected static void uploadConfigToZk(String configDir, String configName) throws Exception {
    // override settings in the solrconfig include
    System.setProperty("solr.tests.maxBufferedDocs", "100000");
    System.setProperty("solr.tests.maxIndexingThreads", "-1");
    System.setProperty("solr.tests.ramBufferSizeMB", "100");
    // use non-test classes so RandomizedRunner isn't necessary
    System.setProperty("solr.tests.mergeScheduler", "org.apache.lucene.index.ConcurrentMergeScheduler");
    System.setProperty("solr.directoryFactory", "solr.RAMDirectoryFactory");
    SolrZkClient zkClient = null;
    try {
      zkClient =  new SolrZkClient(miniCluster.getZkServer().getZkAddress(), TIMEOUT, 45000, null);
      uploadConfigFileToZk(zkClient, configName, "solrconfig.xml", new File(configDir, "solrconfig-tlog.xml"));
      uploadConfigFileToZk(zkClient, configName, "schema.xml", new File(configDir, "schema.xml"));
      uploadConfigFileToZk(zkClient, configName, "solrconfig.snippet.randomindexconfig.xml",
          new File(configDir, "solrconfig.snippet.randomindexconfig.xml"));
      uploadConfigFileToZk(zkClient, configName, "old_synonyms.txt", new File(configDir, "old_synonyms.txt"));
      uploadConfigFileToZk(zkClient, configName, "protwords.txt", new File(configDir, "protwords.txt"));
      uploadConfigFileToZk(zkClient, configName, "stopwords.txt", new File(configDir, "stopwords.txt"));
      uploadConfigFileToZk(zkClient, configName, "synonyms.txt", new File(configDir, "synonyms.txt"));
      uploadConfigFileToZk(zkClient, configName, "stopwords.txt", new File(configDir, "stopwords.txt"));
    }finally{
      if(zkClient != null){
        zkClient.close();
      }
    }
  }

  protected static void uploadConfigFileToZk(SolrZkClient zkClient, String configName, String nameInZk, File file)
      throws Exception {
    zkClient.makePath(CONFIGS_ZKNODE + "/" + configName + "/" + nameInZk, file, false, true);
  }

  private static void createCollection(String name, int numShards,
      int replicationFactor, String configName) throws Exception {
    CloudSolrServer cloudSolrServer = null;
    SolrZkClient zkClient = null;
    QueryRequest request= null;
    try{
      cloudSolrServer = new CloudSolrServer(miniCluster.getZkServer().getZkAddress(), true);
      cloudSolrServer.setDefaultCollection(COLLECTION_NAME);
      zkClient = new SolrZkClient(miniCluster.getZkServer().getZkAddress(),
          TIMEOUT, 45000, null);
      ModifiableSolrParams modParams = new ModifiableSolrParams();
      modParams.set(CoreAdminParams.ACTION, CollectionAction.CREATE.name());
      modParams.set("name", name);
      modParams.set("numShards", numShards);
      modParams.set("replicationFactor", replicationFactor);
      modParams.set("collection.configName", configName);
      request = new QueryRequest(modParams);
      request.setPath("/admin/collections");
      cloudSolrServer.request(request);
      ZkStateReader zkStateReader = new ZkStateReader(zkClient);
      waitForRecoveriesToFinish(COLLECTION_NAME, zkStateReader, true, true, 330);
    }finally{
      if(cloudSolrServer != null){
        cloudSolrServer.shutdown();
      }
    }
  }

  private static void insertDocs() throws SolrServerException, IOException{
    CloudSolrServer cloudSolrServer = null;
    try{
      List<SolrInputDocument> input = new ArrayList<SolrInputDocument>();
      cloudSolrServer = new CloudSolrServer(miniCluster.getZkServer().getZkAddress(), true);
      cloudSolrServer.setDefaultCollection(COLLECTION_NAME);
      cloudSolrServer.connect();
      cloudSolrServer.setDefaultCollection(COLLECTION_NAME);
      populateInputDocList(1, SOLR_DATA[1], input);
      populateInputDocList(2, SOLR_DATA[2], input);
      populateInputDocList(3, SOLR_DATA[3], input);
      populateInputDocList(4, SOLR_DATA[4], input);
      populateInputDocList(5, SOLR_DATA[5], input);
      populateInputDocList(6, SOLR_DATA[6], input);
      cloudSolrServer.add(input);
      cloudSolrServer.commit();
    }finally{
      if(cloudSolrServer != null){
        cloudSolrServer.shutdown();
      }
    }
  }

  private static void populateInputDocList(int id, String name, List<SolrInputDocument> input){
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", id);
    doc.setField("name", name);
    input.add(doc);
  }

  protected static void waitForRecoveriesToFinish(String collection,
      ZkStateReader zkStateReader, boolean verbose, boolean failOnTimeout, int timeoutSeconds)
          throws Exception {
    LOG.info("Wait for recoveries to finish - collection: " + collection + " failOnTimeout:" + failOnTimeout + " timeout (sec):" + timeoutSeconds);
    boolean cont = true;
    int cnt = 0;

    while (cont) {
      if (verbose) System.out.println("-");
      boolean sawLiveRecovering = false;
      zkStateReader.updateClusterState(true);
      ClusterState clusterState = zkStateReader.getClusterState();
      Map<String,Slice> slices = clusterState.getSlicesMap(collection);
      assertNotNull("Could not find collection:" + collection, slices);
      for (Map.Entry<String,Slice> entry : slices.entrySet()) {
        Map<String,Replica> shards = entry.getValue().getReplicasMap();
        for (Map.Entry<String,Replica> shard : shards.entrySet()) {
          if (verbose) System.out.println("rstate:"
              + shard.getValue().getStr(ZkStateReader.STATE_PROP)
              + " live:"
              + clusterState.liveNodesContain(shard.getValue().getNodeName()));
          String state = shard.getValue().getStr(ZkStateReader.STATE_PROP);
          if ((state.equals(ZkStateReader.RECOVERING) || state
              .equals(ZkStateReader.SYNC) || state.equals(ZkStateReader.DOWN))
              && clusterState.liveNodesContain(shard.getValue().getStr(
                  ZkStateReader.NODE_NAME_PROP))) {
            sawLiveRecovering = true;
          }
        }
      }
      if (!sawLiveRecovering || cnt == timeoutSeconds) {
        if (!sawLiveRecovering) {
          if (verbose) System.out.println("no one is recoverying");
        } else {
          if (verbose) System.out.println("Gave up waiting for recovery to finish..");
          if (failOnTimeout) {
            fail("There are still nodes recoverying - waited for " + timeoutSeconds + " seconds");
            // won't get here
            return;
          }
        }
        cont = false;
      } else {
        Thread.sleep(1000);
      }
      cnt++;
    }

    LOG.info("Recoveries finished - collection: " + collection);
  }
}

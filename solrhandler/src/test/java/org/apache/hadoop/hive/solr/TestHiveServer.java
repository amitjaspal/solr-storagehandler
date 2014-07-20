package org.apache.hadoop.hive.solr;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.service.HiveInterface;
import org.apache.hadoop.hive.service.HiveServer;
import org.apache.thrift.TException;

public class TestHiveServer extends TestCase{
    
    /*public static final File HIVE_BASE_DIR = new File("target/hive");
    public static final File HIVE_SCRATCH_DIR = new File(HIVE_BASE_DIR +
"/scratchdir");
    public static final File HIVE_LOCAL_SCRATCH_DIR = new
File(HIVE_BASE_DIR + "/localscratchdir");
    public static final File HIVE_METADB_DIR = new File(HIVE_BASE_DIR +
"/metastoredb");
    public static final File HIVE_LOGS_DIR = new File(HIVE_BASE_DIR +
"/logs");
    public static final File HIVE_TMP_DIR = new File(HIVE_BASE_DIR +
"/tmp");
    public static final File HIVE_WAREHOUSE_DIR = new File(HIVE_BASE_DIR +
"/warehouse");
    public static final File HIVE_TESTDATA_DIR = new File(HIVE_BASE_DIR +
"/testdata");
    public static final File HIVE_HADOOP_TMP_DIR = new File(HIVE_BASE_DIR +
"/hadooptmp");
    protected HiveInterface client;

    protected void setupHive() throws MetaException, IOException,
TException {
        
      
        FileUtils.forceMkdir(HIVE_BASE_DIR);
        FileUtils.forceMkdir(HIVE_SCRATCH_DIR);
        FileUtils.forceMkdir(HIVE_LOCAL_SCRATCH_DIR);
        FileUtils.forceMkdir(HIVE_LOGS_DIR);
        FileUtils.forceMkdir(HIVE_TMP_DIR);
        FileUtils.forceMkdir(HIVE_WAREHOUSE_DIR);
        FileUtils.forceMkdir(HIVE_HADOOP_TMP_DIR);
        FileUtils.forceMkdir(HIVE_TESTDATA_DIR);

        System.setProperty("javax.jdo.option.ConnectionURL",
"jdbc:derby:;databaseName=" + HIVE_METADB_DIR.getAbsolutePath() +
";create=true");
        System.setProperty("hive.metastore.warehouse.dir",
HIVE_WAREHOUSE_DIR.getAbsolutePath());
        System.setProperty("hive.exec.scratchdir",
HIVE_SCRATCH_DIR.getAbsolutePath());
        System.setProperty("hive.exec.local.scratchdir",
HIVE_LOCAL_SCRATCH_DIR.getAbsolutePath());
        System.setProperty("hive.metastore.metadb.dir",
HIVE_METADB_DIR.getAbsolutePath());
        System.setProperty("test.log.dir",
HIVE_LOGS_DIR.getAbsolutePath());
        System.setProperty("hive.querylog.location",
HIVE_TMP_DIR.getAbsolutePath());
        System.setProperty("hadoop.tmp.dir",
HIVE_HADOOP_TMP_DIR.getAbsolutePath());
        System.setProperty("derby.stream.error.file",
HIVE_BASE_DIR.getAbsolutePath() + "/derby.log");

        client = new HiveServer.HiveServerHandler();

    }


    
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        client.clean();
        
    }*/
    
    public void testSolrStorageHandler()
    {
        assertTrue( true );
    }
}

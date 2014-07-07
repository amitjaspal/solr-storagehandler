package org.apache.hadoop.hive.solr;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;

public class SolrInputFormat implements InputFormat<LongWritable, MapWritable>{
    
    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits){
        
        CloudSolrServer cloudServer = null;
        ZkStateReader stateReader; 
        Collection<Slice> slices;
        
        // ToDo : remove hard-coding for the path.
        Path path = new Path("hdfs://localhost:8020/user/hive/warehouse/");
        
        String zooKeeperAddress = job.get(ExternalTableProperties.ZOOKEEPER_SERVICE_URL);
        try{
            cloudServer = new CloudSolrServer(zooKeeperAddress);
        }catch(MalformedURLException ex){
            ex.printStackTrace();
        }
        cloudServer.connect();
        stateReader = cloudServer.getZkStateReader();
        ClusterState cs = stateReader.getClusterState();
        slices = cs.getSlices(job.get(ExternalTableProperties.COLLECTION_NAME));
        InputSplit []inputSplits = new HiveSolrSplit[slices.size()];
        int i = 0;
        for(Slice slice : slices){
            
            Replica leader = slice.getLeader();
            SolrInputSplit split = new SolrInputSplit(leader.getProperties().get("base_url").toString(), leader.getProperties().get("core").toString()
                                          , job.get(ExternalTableProperties.COLLECTION_NAME));
            inputSplits[i] = new HiveSolrSplit(split, path);
            i++;
        }
        
        stateReader.close();
        return inputSplits;
    }
    
    @Override
    public RecordReader<LongWritable, MapWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter){
        
        HiveSolrSplit hiveSolrSplit = (HiveSolrSplit)split;
        SolrInputSplit solrInputSplit = hiveSolrSplit.getSolrSplit();
        SolrDAO solrDAO = new SolrDAO(solrInputSplit.getNodeURL(), solrInputSplit.getShardName(), 
                                      solrInputSplit.getCollectionName());
        
        // ToDo: we need to inspect the searchConditions  and form a SOLR corresponding query
        // ToDo: Instantiate SolrDAO with the query passed as a parameter to the constructor
        // There are two parts to query 
        // 1. Query passed while creating table.
        // 2. Filter query while adding predicate pushdown.
        SolrQuery solrQuery = QueryBuilder.buildQuery(job);
        solrDAO.setQuery(solrQuery);
        return new SolrRecordReader(split, solrDAO);
    }
    
    /*
     * This method initializes the PredicateAnalyzer that is consumed 
     * by the Hive query optimizer while evaluating the query.
     * We can plug in predicates to be pushed into SOLR in this method.
     * A know limitation of predicate pushdown is that only predicates with
     * conjunctions will be pushed at the SOLR level.
     */
    public static IndexPredicateAnalyzer getPredicateAnalyzer(){
        
        IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan");
        return analyzer;
    }

    private String formSolrQuery(JobConf job){
        
        String filterExprSerialized = job.get(TableScanDesc.FILTER_EXPR_CONF_STR);
        if(filterExprSerialized == null) return null;
        ExprNodeDesc filterExpr = Utilities.deserializeExpression(filterExprSerialized);
        IndexPredicateAnalyzer analyzer = getPredicateAnalyzer();
        List<IndexSearchCondition> searchConditions = new ArrayList<IndexSearchCondition>();
        ExprNodeDesc residualPredicate = analyzer.analyzePredicate(filterExpr, searchConditions);
        for (IndexSearchCondition condition : searchConditions){
            if (condition.getComparisonOp().equals("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan")) {
                System.out.println("Greater Than Operator Found");
            }
        }
        return "";
    }
}

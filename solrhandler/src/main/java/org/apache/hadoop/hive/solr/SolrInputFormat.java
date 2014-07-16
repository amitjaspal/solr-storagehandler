package org.apache.hadoop.hive.solr;

import java.net.MalformedURLException;
import java.util.Collection;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.FileInputFormat;
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
        Path []result = FileInputFormat.getInputPaths(job);
        Path path = result[0];
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
        InputSplit []inputSplits = new HiveSolrInputSplit[slices.size()];
        int i = 0;
        for(Slice slice : slices){
            
            Replica leader = slice.getLeader();
            SolrInputSplit split = new SolrInputSplit(leader.getProperties().get("base_url").toString(), leader.getProperties().get("core").toString()
                                          , job.get(ExternalTableProperties.COLLECTION_NAME));
            inputSplits[i] = new HiveSolrInputSplit(split, path);
            i++;
        }
        
        stateReader.close();
        return inputSplits;
    }
    
    @Override
    public RecordReader<LongWritable, MapWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter){
        
        HiveSolrInputSplit hiveSolrSplit = (HiveSolrInputSplit)split;
        SolrInputSplit solrInputSplit = hiveSolrSplit.getSolrSplit();
        SolrQuery solrQuery = QueryBuilder.buildQuery(job);
        SolrDAO solrDAO = new SolrDAO(solrInputSplit.getNodeURL(), solrInputSplit.getShardName(), 
                                      solrInputSplit.getCollectionName(), solrQuery);
        
        solrDAO.setQuery(solrQuery);
        return new SolrRecordReader(split, solrDAO);
    }
}

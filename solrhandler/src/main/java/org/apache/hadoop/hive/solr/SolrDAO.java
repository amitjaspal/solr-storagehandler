package org.apache.hadoop.hive.solr;

import java.util.Collection;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

public class SolrDAO{

    private String nodeURL;
    private String shardName;
    private String collectionName;
    private HttpSolrServer solrServer;
    private SolrDocumentList resultSet;
    private Integer currentPosition;
    private SolrQuery query;
    
    SolrDAO(String nodeURL, String shardName, String collectionName){
        this.nodeURL = nodeURL;
        this.shardName = shardName;
        this.collectionName = collectionName;
        this.solrServer = new HttpSolrServer(this.nodeURL + "/" + this.shardName);
        this.currentPosition = 0;
    }
 
    public void setQuery(SolrQuery query){
        this.query = query;
    }

    public void executeQuery(){

        QueryResponse response = null;
        System.out.println("Executing Query !!");
        try{
            response = solrServer.query(query);
            
        }catch( SolrServerException e){
            // do some logging about the failure of the query.
        }
        resultSet = response.getResults();
        System.out.println("result size == " + resultSet.size());
    }
    
    public SolrDocument getNextDoc(){
        
        
        if(currentPosition == 0){
            executeQuery();
        }
        if(resultSet == null || currentPosition >= resultSet.size()) return null;
        
        SolrDocument nextDoc = resultSet.get(currentPosition);
        currentPosition++;
        return nextDoc;
    }
    
    public void saveDocs(Collection<SolrInputDocument> docs){
        try{
            solrServer.add(docs);
            solrServer.commit();
        }catch(Exception ex){
            // Log errors.
        }
    }
    
    public long getLength(){
        return resultSet.size();
    }

    public String getNodeURL() {
        return nodeURL;
    }

    public void setNodeURL(String nodeURL) {
        this.nodeURL = nodeURL;
    }

    public String getShardName() {
        return shardName;
    }

    public void setShardName(String shardName) {
        this.shardName = shardName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }
}

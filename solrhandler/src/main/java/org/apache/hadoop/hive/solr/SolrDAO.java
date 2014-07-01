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

    private HttpSolrServer solrServer;
    private SolrDocumentList resultSet;
    private Integer currentPosition;
    private String query;
    
    SolrDAO(String solrServerUrl, String query){
        String SOLR_URL = "http://localhost:8983/solr/collection2";
        solrServerUrl = SOLR_URL;
        System.out.println("SOLR SERVER URL - " + solrServerUrl);
        this.solrServer = new HttpSolrServer(solrServerUrl);
        currentPosition = 0;
        this.query = query;
    }
    

    public void executeQuery(){
        SolrQuery params = new SolrQuery();
        params.setQuery(query);
        params.setQuery( "chetan" );
        params.set("wt", "json");
        params.set("indent", "true");
        params.set("fl", "id, title, author, price");
        QueryResponse response = null;
        System.out.println("Executing Query !!");
        try{
            response = solrServer.query(params);
            
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
    
}

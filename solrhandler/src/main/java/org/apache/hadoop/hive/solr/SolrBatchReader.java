package org.apache.hadoop.hive.solr;

import java.util.concurrent.CyclicBarrier;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;

class SolrBatchReader implements Runnable{
    
    private Integer start;
    private Integer window;
    private Long size;
    private SolrQuery query;
    private SolrDocumentList buffer;
    private HttpSolrServer solrServer;
    private CyclicBarrier cb;
    
    SolrBatchReader(Integer start, Integer window, Long size, SolrQuery query,
                          HttpSolrServer solrServer, SolrDocumentList buffer, CyclicBarrier cb) {
        this.start = start;
        this.window = window;
        this.size = size;
        this.query = query;
        this.solrServer = solrServer;
        this.buffer = buffer;
        this.cb = cb;
    }
    
    public void run(){
       
        QueryResponse response = null;
        buffer.clear();
        try{
            query.setStart(start);
            query.setRows(window);
            response = solrServer.query(query);
        }catch( SolrServerException e){
            e.printStackTrace();
        }
        buffer.addAll(response.getResults());
        System.out.println("result size == " + buffer.size());
        try{
            cb.await();
        }catch(Exception ex){ // TODO: Catch proper exceptions
            ex.printStackTrace();
        }
    }
}
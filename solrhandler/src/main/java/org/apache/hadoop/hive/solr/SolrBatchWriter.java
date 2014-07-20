package org.apache.hadoop.hive.solr;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

class SolrBatchWriter implements Runnable{
    
    private List<SolrInputDocument> outputBuffer;
    private HttpSolrServer solrServer;
    private CyclicBarrier writerCB;
    
    SolrBatchWriter(HttpSolrServer solrServer, List<SolrInputDocument> buffer, CyclicBarrier readerCB) {
        this.solrServer = solrServer;
        this.outputBuffer = buffer;
        this.writerCB = readerCB;
    }
    
    public void run(){
       
        try{
            solrServer.add(outputBuffer);
        }catch(SolrServerException e){
            e.printStackTrace();
        }
        catch(IOException e){
            e.printStackTrace();
        }
        try{
            writerCB.await();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
package org.apache.hadoop.hive.solr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

class SolrDocumentExtracter implements Runnable{
    
    private Integer start;
    private Integer window;
    private Long size;
    private SolrQuery query;
    private SolrDocumentList buffer;
    private HttpSolrServer solrServer;
    private CyclicBarrier cb;
    
    SolrDocumentExtracter(Integer start, Integer window, Long size, SolrQuery query,
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

public class SolrDAO{

    private String nodeURL;
    private String shardName;
    private String collectionName;
    private HttpSolrServer solrServer;
    private SolrDocumentList resultSet;
    private SolrDocumentList inputBuffer;
    private List<SolrInputDocument> outputBuffer;
    private Integer currentPosition;
    private Integer start;
    private Integer window;
    private Long size;
    private SolrQuery query;
    private Thread T;
    private CyclicBarrier cb;
    
    SolrDAO(String nodeURL, String shardName, String collectionName, SolrQuery query){
        this.nodeURL = nodeURL;
        this.shardName = shardName;
        this.collectionName = collectionName;
        this.solrServer = new HttpSolrServer(this.nodeURL + "/" + this.shardName);
        this.currentPosition = 0;
        this.query = query;
        this.start = 0;
        this.window = 1000;
        if(query != null){
            initSize();
            this.inputBuffer = new SolrDocumentList();
            this.resultSet = new SolrDocumentList();
            this.cb=new CyclicBarrier(2);
            T = new Thread(new SolrDocumentExtracter(start, window, size, query, solrServer, inputBuffer,cb));
            T.start();
        }else{
            this.outputBuffer = new ArrayList<SolrInputDocument>();
        }
    }
 
    public void setQuery(SolrQuery query){
         this.query = query;
    }
    
    private void initSize(){
        try{
            
            query.setRows(0);  // don't actually request any data
            size = solrServer.query(query).getResults().getNumFound();
            System.out.println("size for the query results = " + size);
        }catch(SolrServerException e){
            e.printStackTrace();
        }
    }
   
    public SolrDocument getNextDoc(){
        
        if(currentPosition >= size){
            try{
                cb.await();
            }catch(Exception ex){
                ex.printStackTrace();
            }
            return null;
        }
        
        if(currentPosition % window == 0){
            resultSet.clear();
            try{
                cb.await();
            }catch(Exception ex){
                ex.printStackTrace();
            }
            cb.reset();
            for(int i = 0;i<inputBuffer.size();i++){
                resultSet.add(inputBuffer.get(i));
            }
            inputBuffer.clear();
            start = start + window;
            T = new Thread(new SolrDocumentExtracter(start, window, size, query, solrServer, inputBuffer,cb));
            T.start();
        }
        
        SolrDocument nextDoc = resultSet.get(currentPosition % window);
        currentPosition++;
        return nextDoc;
    }
    
    public void saveDocs(Collection<SolrInputDocument> docs){
        try{
            solrServer.add(docs);
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
    
    public void saveDoc(SolrInputDocument doc){
        outputBuffer.add(doc);
    }
    
    public void commit(){
        
        try{
            solrServer.add(outputBuffer);
            solrServer.commit();
        }catch(SolrServerException e){
            e.printStackTrace();
        }
        catch(IOException e){
            e.printStackTrace();
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

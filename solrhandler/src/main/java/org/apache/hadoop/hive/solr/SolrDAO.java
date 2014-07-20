package org.apache.hadoop.hive.solr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

public class SolrDAO{

    private String nodeURL;
    private String shardName;
    private String collectionName;
    private HttpSolrServer solrServer;
    private SolrDocumentList inputDocs;
    private SolrDocumentList inputBuffer;
    private List<SolrInputDocument> outputBuffer;
    private List<SolrInputDocument> outputDocs;
    private Integer currentPosition;
    private Integer start; // TODO: Move the start and window to job configuration
    private Integer window;// rather than hard coding it here.  
    private Long size;
    private SolrQuery query;
    private Thread T;
    private CyclicBarrier readerCB;
    private CyclicBarrier writerCB;
    private Boolean isWriterThreadInitiated;
    
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
            this.inputDocs = new SolrDocumentList();
            this.readerCB=new CyclicBarrier(2);
            T = new Thread(new SolrBatchReader(start, window, size, query, solrServer, inputBuffer,readerCB));
            T.start();
        }else{
            this.outputBuffer = new ArrayList<SolrInputDocument>();
            this.outputDocs = new ArrayList<SolrInputDocument>();
            this.writerCB=new CyclicBarrier(2);
            this.isWriterThreadInitiated = false;
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
                readerCB.await();
            }catch(Exception ex){
                ex.printStackTrace();
            }
            return null;
        }
        
        if(currentPosition % window == 0){
            inputDocs.clear();
            try{
                readerCB.await();
            }catch(Exception ex){
                ex.printStackTrace();
            }
            readerCB.reset();
            for(int i = 0;i<inputBuffer.size();i++){
                inputDocs.add(inputBuffer.get(i));
            }
            inputBuffer.clear();
            start = start + window;
            T = new Thread(new SolrBatchReader(start, window, size, query, solrServer, inputBuffer,readerCB));
            T.start();
        }
        
        SolrDocument nextDoc = inputDocs.get(currentPosition % window);
        currentPosition++;
        return nextDoc;
    }
    
    public void saveDoc(SolrInputDocument doc){
        outputDocs.add(doc);
        
        if(outputDocs.size() == window){
            if(isWriterThreadInitiated){
                try{
                    writerCB.await();
                }catch(Exception ex){
                    ex.printStackTrace();
                }
            
                writerCB.reset();
            }
            outputBuffer.clear();
            for(int i = 0;i<outputDocs.size();i++){
                outputBuffer.add(outputDocs.get(i));
            }
            outputDocs.clear();
            T = new Thread(new SolrBatchWriter(solrServer, outputBuffer,writerCB));
            T.start();
            isWriterThreadInitiated = true;
        }
    }
    
    public void commit(){
        
        try{
            if(outputDocs.size() > 0){
                solrServer.add(outputDocs);
            }    
            solrServer.commit();
        }catch(SolrServerException e){
            e.printStackTrace();
        }
        catch(IOException e){
            e.printStackTrace();
        }
        isWriterThreadInitiated = false;
    }
    
    public long getLength(){
        return inputDocs.size();
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

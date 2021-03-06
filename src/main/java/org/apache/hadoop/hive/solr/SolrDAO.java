/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.solr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

/*
 * SolrDAO acts as a solr data access object and uses solrj api
 * to provide read and write operations to SOLR. SolrDAO spawns
 * SolrDAO spawns SolrBatchReader and SolrBatchWriter to perform
 * read and write operations. SolrBatchReader thread reads data
 * in a moving window fashion such that it is always 1 window
 * ahead of the values read by the SolrRecordReader.
 */
public class SolrDAO{

  private static final Logger LOG = Logger.getLogger(SolrDAO.class.getName());
  private final String nodeURL;
  private final String shardName;
  private final String collectionName;
  private final HttpSolrServer solrServer;
  private SolrDocumentList inputDocs;
  private SolrDocumentList inputBuffer;
  private List<SolrInputDocument> outputBuffer;
  private List<SolrInputDocument> outputDocs;
  private Integer currentPosition;
  private Integer start; // TODO: Move the start and window to job configuration
  private final Integer window;// rather than hard coding it here.
  private Long size;
  private SolrQuery query;
  private Thread T;
  private CyclicBarrier readerCB;
  private CyclicBarrier writerCB;
  private Boolean isWriterThreadInitiated;
  private StringBuffer nextCursorMark;

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
      this.nextCursorMark = new StringBuffer();
      this.nextCursorMark.append("*");
      LOG.debug("Starting the SolrBatchReader thread for start = " + start + ", window = " + window);
      T = new Thread(new SolrBatchReader(start, window, size, query, solrServer, inputBuffer,readerCB, nextCursorMark));
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
      LOG.debug("size for the query results = " + size);
    }catch(SolrServerException ex){
      LOG.log(Level.ERROR, "Exception occured while querying the solr server", ex);
    }
  }

  public SolrDocument getNextDoc(){

    if(currentPosition >= size){
      try{
        readerCB.await();
      }catch(InterruptedException ex) {
        Thread.currentThread().interrupt();
      }catch(BrokenBarrierException ex) {
        LOG.log(Level.ERROR, "Exception occured while waiting on cyclic buffer", ex);
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
      inputDocs.addAll(inputBuffer);
      inputBuffer.clear();
      start = start + window;
      LOG.debug("Starting the SolrBatchReader thread for start = " + start + ", window = " + window);
      T = new Thread(new SolrBatchReader(start, window, size, query, solrServer, inputBuffer,readerCB, nextCursorMark));
      T.start();
    }

    SolrDocument nextDoc = inputDocs.get(currentPosition % window);
    currentPosition++;
    return nextDoc;
  }

  public float getProgress(){
    if(size == 0) return 0.0f;
    return (start + currentPosition) / (size) ;
  }
  
  public void saveDoc(SolrInputDocument doc){
    outputDocs.add(doc);

    if(outputDocs.size() == window){
      if(isWriterThreadInitiated){
        try{
          writerCB.await();
        }catch(BrokenBarrierException ex){
          LOG.log(Level.ERROR, "Exception occured while waiting on cyclic buffer", ex);
        }catch(InterruptedException ex){
          Thread.currentThread().interrupt();
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
}

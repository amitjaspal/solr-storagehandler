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

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;

/*
 * SolrBatchReader reads results in a batch fashion, it queries the Solr
 * shards only for a particular range of documents decided by the start and
 * the window paraments.
 */
class SolrBatchReader implements Runnable{

  private static final Logger LOG = Logger.getLogger(SolrBatchReader.class.getName());
  private final Integer start;
  private final Integer window;
  private final Long size;
  private final SolrQuery query;
  private final SolrDocumentList buffer;
  private final HttpSolrServer solrServer;
  private final CyclicBarrier cb;
  private final StringBuffer nextCursorMark;

  SolrBatchReader(Integer start, Integer window, Long size, SolrQuery query,
      HttpSolrServer solrServer, SolrDocumentList buffer, CyclicBarrier cb, StringBuffer nextCursorMark) {
    this.start = start;
    this.window = window;
    this.size = size;
    this.query = query;
    this.solrServer = solrServer;
    this.buffer = buffer;
    this.cb = cb;
    this.nextCursorMark = nextCursorMark;
  }

  @Override
  public void run(){

    QueryResponse response = null;
    buffer.clear();
    // Read data from the SOLR server.
    try{
      query.setRows(window);
      query.set("sort", "score desc,id asc");
      query.set("cursorMark",nextCursorMark.toString());
      response = solrServer.query(query);
      nextCursorMark.delete(0, nextCursorMark.length());
      nextCursorMark.append(response.getNextCursorMark());
    }catch( SolrServerException ex){
      LOG.log(Level.ERROR, "Exception occured while querying the solr server", ex);
    }
    buffer.addAll(response.getResults());
    // Signal the parent thread that I am done.
    try{
      cb.await();
    }catch(BrokenBarrierException ex){ // TODO: Catch proper exceptions
      LOG.log(Level.ERROR, "Exception occured while waiting on cyclic buffer", ex);
    }catch(InterruptedException ex){
      // Restore the interrupted status
      Thread.currentThread().interrupt();
    }
  }
}
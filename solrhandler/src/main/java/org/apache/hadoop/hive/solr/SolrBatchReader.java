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
        // Read data from the SOLR server.
        try{
            query.setStart(start);
            query.setRows(window);
            response = solrServer.query(query);
        }catch( SolrServerException e){
            e.printStackTrace();
        }
        buffer.addAll(response.getResults());
        // Signal the parent thread that I am done.
        try{
            cb.await();
        }catch(Exception ex){ // TODO: Catch proper exceptions
            ex.printStackTrace();
        }
    }
}
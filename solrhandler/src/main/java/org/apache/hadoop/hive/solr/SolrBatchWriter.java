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
        // add the buffer to the SOLR Server
        try{
            solrServer.add(outputBuffer);
        }catch(SolrServerException e){
            e.printStackTrace();
        }
        catch(IOException e){
            e.printStackTrace();
        }
        // Signal the parent thread that I am done.
        try{
            writerCB.await();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
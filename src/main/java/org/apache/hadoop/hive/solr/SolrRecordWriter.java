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
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.solr.common.SolrInputDocument;

/*
 * SolrRecordWriter is used to write data to SOLR.
 * Under the hood it just calls SolrDAO methods to
 * do this job.
 */
public class SolrRecordWriter implements RecordWriter{

  private final SolrDAO solrDAO;

  public SolrRecordWriter(SolrDAO solrDAO){
    this.solrDAO = solrDAO;
  }

  @Override
  public void write(Writable wrt) throws IOException{
    MapWritable tuple = (MapWritable) wrt;
    SolrInputDocument doc = new SolrInputDocument();
    for(Map.Entry<Writable, Writable> entry:tuple.entrySet()){
      doc.setField(entry.getKey().toString(), entry.getValue().toString());
    }
    solrDAO.saveDoc(doc);
    return ;
  }

  @Override
  public void close(boolean f) throws IOException{
    solrDAO.commit();
  }
}
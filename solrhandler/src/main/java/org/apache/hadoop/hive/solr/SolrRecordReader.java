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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.solr.common.SolrDocument;

public class SolrRecordReader implements RecordReader<LongWritable, MapWritable>{

  private final SolrDAO solrDAO;
  private final Integer currentPosition;

  public SolrRecordReader(InputSplit solrSplit, SolrDAO solrDAO){
    this.currentPosition = 0;
    this.solrDAO = solrDAO;
  }

  @Override
  public void close() throws IOException{
  }

  @Override
  public LongWritable createKey(){
    return new LongWritable();
  }

  @Override
  public MapWritable createValue(){
    return new MapWritable();
  }

  @Override
  public long getPos() throws IOException{
    return this.currentPosition;
  }

  @Override
  public float getProgress() throws IOException{
    if(solrDAO.getLength() == 0) return 0.0f;
    return currentPosition / solrDAO.getLength();
  }

  @Override
  public boolean next(LongWritable key, MapWritable value){
    SolrDocument doc = solrDAO.getNextDoc();
    if( doc == null) {
      return false;
    }else{
      key.set(currentPosition);
      for(String field : doc.getFieldNames()){
        String val = doc.getFieldValue(field).toString();
        value.put(new Text(field), new Text(val));
      }
      return true;
    }
  }

}


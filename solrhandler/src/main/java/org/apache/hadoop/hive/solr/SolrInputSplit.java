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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;

/*
 * SolrInputSplit defines the properties corresponding to each Input Split.
 * We are currently considering each shard of the collection to act as a
 * split. So data will be read from each shard in parallel by individual
 * mapper's when a select query is fired.
 */
public class SolrInputSplit implements InputSplit{

  private String nodeURL;
  private String shardName;
  private String collectionName;

  SolrInputSplit(){

  }

  SolrInputSplit(String nodeURL, String shardName, String collectionName){
    this.nodeURL = nodeURL;
    this.shardName = shardName;
    this.collectionName = collectionName;
  }

  @Override
  public long getLength() throws IOException {
    return 1l;
  }

  @Override
  public String[] getLocations() throws IOException {
    return new String[]{ };
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    nodeURL = in.readUTF();
    shardName = in.readUTF();
    collectionName = in.readUTF();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(nodeURL);
    out.writeUTF(shardName);
    out.writeUTF(collectionName);
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

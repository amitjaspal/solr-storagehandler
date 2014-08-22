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

import java.net.MalformedURLException;
import java.util.Collection;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;

public class SolrInputFormat implements InputFormat<LongWritable, MapWritable>{

  private static final Logger LOG = Logger.getLogger(SolrInputFormat.class);

  /*
   * This method queries SOLR Cloud for the number of shards the collection has,
   * one split per shard is created.
   */
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws MalformedURLException{

    CloudSolrServer cloudServer = null;
    ZkStateReader stateReader;
    Path []result = FileInputFormat.getInputPaths(job);
    Path path = result[0];
    String zooKeeperAddress = job.get(ExternalTableProperties.ZOOKEEPER_SERVICE_URL);
    cloudServer = new CloudSolrServer(zooKeeperAddress);
    cloudServer.setDefaultCollection(job.get(ExternalTableProperties.COLLECTION_NAME));
    cloudServer.connect();
    stateReader = cloudServer.getZkStateReader();
    ClusterState cs = stateReader.getClusterState();
    Collection<Slice> slices = cs.getSlices(job.get(ExternalTableProperties.COLLECTION_NAME));
    InputSplit []inputSplits = new SolrFileSplit[slices.size()];
    int i = 0;
    for(Slice slice : slices){
      Replica leader = slice.getLeader();
      SolrInputSplit split = new SolrInputSplit(leader.getProperties().get("base_url").toString(), leader.getProperties().get("core").toString()
          , job.get(ExternalTableProperties.COLLECTION_NAME));
      inputSplits[i] = new SolrFileSplit(split, path);
      i++;
    }
    LOG.debug("solr splits size = "+ inputSplits.length);
    stateReader.close();
    return inputSplits;
  }

  @Override
  public RecordReader<LongWritable, MapWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter){

    SolrFileSplit hiveSolrSplit = (SolrFileSplit)split;
    SolrInputSplit solrInputSplit = hiveSolrSplit.getSolrSplit();
    SolrQuery solrQuery = SolrQueryGenerator.generateQuery(job);
    SolrDAO solrDAO = new SolrDAO(solrInputSplit.getNodeURL(), solrInputSplit.getShardName(),
        solrInputSplit.getCollectionName(), solrQuery);
    solrDAO.setQuery(solrQuery);
    return new SolrRecordReader(split, solrDAO);
  }
}

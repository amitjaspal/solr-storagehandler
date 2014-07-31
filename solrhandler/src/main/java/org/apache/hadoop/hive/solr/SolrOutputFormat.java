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
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;

import com.sun.rowset.internal.Row;



@SuppressWarnings("rawtypes")
public class SolrOutputFormat implements HiveOutputFormat<NullWritable, Row>
 {

    @Override
    public org.apache.hadoop.mapred.RecordWriter getRecordWriter(FileSystem ignored, JobConf job,
            String name, Progressable progress) throws IOException {
        // Hive will not call this method.
        return null;
    }

    @Override
    public RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
            final Class<? extends Writable> valueClass, boolean isCompressed,
            Properties tableProperties, Progressable progress) throws IOException{

        // Need to figure out how to improve the degree of parallelism.
        // For now we will just have 1 shard insert all the documents.

        CloudSolrServer cloudServer = null;
        ZkStateReader stateReader;
        Collection<Slice> slices;
        String zooKeeperAddress = jc.get(ExternalTableProperties.ZOOKEEPER_SERVICE_URL);
        try{
            cloudServer = new CloudSolrServer(zooKeeperAddress);
        }catch(Exception ex){
            ex.printStackTrace();
        }
        cloudServer.connect();
        stateReader = cloudServer.getZkStateReader();
        ClusterState cs = stateReader.getClusterState();
        Slice s = cs.getSlice(jc.get(ExternalTableProperties.COLLECTION_NAME), "shard1");
        Replica r = s.getLeader();
        String baseURL = r.getProperties().get("base_url").toString();
        String shardName = r.getProperties().get("core").toString();;
        String collectionName = jc.get(ExternalTableProperties.COLLECTION_NAME);
        SolrDAO solrDAO = new SolrDAO(baseURL, shardName, collectionName, null);
        return new SolrRecordWriter(solrDAO);
    }

    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf jc) throws IOException{
       //TODO: check if collection name, zookeeper service is set in JobConf.

    }


}

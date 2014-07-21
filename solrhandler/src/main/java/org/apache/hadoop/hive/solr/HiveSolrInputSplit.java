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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;

/*
 * HiveSolrInputSplit is just a wrapper class on top of SolrSplit. It seems 
 * Hive considers all data sources to be of FileFormat so we need to define
 * the wrapper class HiveSolrSplit which extends from FileSplit.
 * Under the hood all functionalities are delegated to SolrSplit 
 * only.
 */

class HiveSolrInputSplit extends FileSplit {
    
    private SolrInputSplit solrSplit;
    private Path path;
    
    public HiveSolrInputSplit(){
       this(new SolrInputSplit(), new Path(" "));
    }
    
    HiveSolrInputSplit(SolrInputSplit solrSplit, Path path){
        super(path, 0, 0, (String[]) null);
        this.solrSplit = solrSplit;
        this.path = path;
    }
    
    public long getLength() {
        return 1l;
    }
    
    public String[] getLocations() throws IOException{
        return solrSplit.getLocations();
    }
    
    public void write(DataOutput out) throws IOException{
        Text.writeString(out, path.toString());
        solrSplit.write(out);
    }
    
    public void readFields(DataInput in) throws IOException {
        path = new Path(Text.readString(in));
        solrSplit.readFields(in);
    }

    @Override
    public String toString() {
        return solrSplit.toString();
    }

    @Override
    public Path getPath() {
        return path;
    }
    
    public SolrInputSplit getSolrSplit(){
        return this.solrSplit;
    }
}

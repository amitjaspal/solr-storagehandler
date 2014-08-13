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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;




public class SolrStorageHandler implements HiveStorageHandler, HiveStoragePredicateHandler{

    private Configuration conf;
    static final Log LOG = LogFactory.getLog(SolrStorageHandler.class);

    @Override
    public Configuration getConf(){
        return this.conf;
    }

    @Override
    public void setConf(Configuration conf){
        this.conf = conf;
    }

    @Override
    public Class<? extends InputFormat> getInputFormatClass(){
        return SolrInputFormat.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass(){
        return SolrOutputFormat.class;
    }

    @Override
    public HiveMetaHook getMetaHook(){
        return new SolrMetaHook();
    }

    @Override
    public Class<? extends SerDe> getSerDeClass(){
        return SolrSerDe.class;
    }

    @Override
    public HiveAuthorizationProvider getAuthorizationProvider(){
        return null;
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties){
        Properties externalTableProperties = tableDesc.getProperties();
        ExternalTableProperties.initialize(externalTableProperties, jobProperties, tableDesc);
    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties){
        Properties externalTableProperties = tableDesc.getProperties();
        ExternalTableProperties.initialize(externalTableProperties, jobProperties, tableDesc);
    }

    @Override
    public void configureJobConf(TableDesc tableDesc, JobConf jobConf){
        // do nothing;
    }

    @Override
    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties){
        // do nothing;
    }

    @Override
    public DecomposedPredicate decomposePredicate(JobConf entries, Deserializer deserializer, ExprNodeDesc exprNodeDesc ){
        IndexPredicateAnalyzer analyzer = PredicateAnalyzer.getPredicateAnalyzer();
        List<IndexSearchCondition> searchConditions = new ArrayList<IndexSearchCondition>();
        ExprNodeDesc residualPredicate = analyzer.analyzePredicate(exprNodeDesc, searchConditions);
        DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
        decomposedPredicate.pushedPredicate = analyzer.translateSearchConditions(searchConditions);
        decomposedPredicate.residualPredicate = (ExprNodeGenericFuncDesc) residualPredicate;
        return decomposedPredicate;
    }

}

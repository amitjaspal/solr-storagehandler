package org.apache.hadoop.hive.solr;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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

/*
 * SolrStorageHandler is used to plug-in SOLR backed data
 * sources in Hive.
 * ToDo: 1. Remove the deprecated SerDe, DeSerializer. 
 */

public class SolrStorageHandler implements HiveStorageHandler, HiveStoragePredicateHandler{
    
    private Configuration conf;
    
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
        return null;
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
        ExternalTableProperties.configureExternalTableProperties(externalTableProperties, jobProperties, tableDesc);
    }
    
    @Override
    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties){
        Properties externalTableProperties = tableDesc.getProperties();
        ExternalTableProperties.configureExternalTableProperties(externalTableProperties, jobProperties, tableDesc);
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
        IndexPredicateAnalyzer analyzer = SolrInputFormat.getPredicateAnalyzer();
        List<IndexSearchCondition> searchConditions = new ArrayList<IndexSearchCondition>();
        ExprNodeDesc residualPredicate = analyzer.analyzePredicate(exprNodeDesc, searchConditions);
        DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
        decomposedPredicate.pushedPredicate = analyzer.translateSearchConditions(searchConditions);

        // Need to check this, base class reference should be able to point
        // to derived class without a cast.
        decomposedPredicate.residualPredicate = (ExprNodeGenericFuncDesc) residualPredicate;
        return decomposedPredicate;
    }
    
}

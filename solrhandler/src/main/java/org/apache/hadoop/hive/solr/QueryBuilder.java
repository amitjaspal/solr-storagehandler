package org.apache.hadoop.hive.solr;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.mapred.JobConf;
import org.apache.solr.client.solrj.SolrQuery;

public class QueryBuilder {
    
    public static SolrQuery buildQuery(JobConf job){
        SolrQuery solrQuery = new SolrQuery();
        String query = job.get(ExternalTableProperties.SOLR_QUERY);
        solrQuery.setQuery(query);
        String fields = StringUtils.join(ExternalTableProperties.COLUMN_NAMES, ", ");
        solrQuery.set("fl", fields);
        // Since each mapper is going to query each shard separately
        // we set "distrib" --> false.
        solrQuery.set("distrib", "false"); 
        // pass the filter query by doing predicate pushdown.
        String filterExprSerialized = job.get(TableScanDesc.FILTER_EXPR_CONF_STR);
        if(filterExprSerialized == null) {
            // If no predicate pushdown is possible
            return solrQuery;
        }
        
        ExprNodeDesc filterExpr = Utilities.deserializeExpression(filterExprSerialized);
        IndexPredicateAnalyzer analyzer = PredicateAnalyzer.getPredicateAnalyzer();
        List<IndexSearchCondition> searchConditions = new ArrayList<IndexSearchCondition>();
        ExprNodeDesc residualPredicate = analyzer.analyzePredicate(filterExpr, searchConditions);
        for (IndexSearchCondition condition : searchConditions){
            
            if (condition.getComparisonOp().equals("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual")){
                String fieldName = condition.getColumnDesc().getColumn();
                String value = condition.getConstantDesc().getValue().toString();
                // Formulating Filter Query Expression.
                StringBuffer fqExp = new StringBuffer();
                fqExp.append(fieldName).append(":").append(value);
                solrQuery.setFilterQueries(fqExp.toString());
            }
            
            // Don't know if there is a way to differentiate between > and >= in SOLR
            // It wont effect the end result since range query in SOLR is inclusive
            // and hive will anyway run the predicate checks afterwards.
            if (condition.getComparisonOp().equals("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan")
                    || condition.getComparisonOp().equals("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan")) {
                String fieldName = condition.getColumnDesc().getColumn();
                String value = condition.getConstantDesc().getValue().toString();
                // Formulating Filter Query Expression.
                StringBuffer fqExp = new StringBuffer();
                fqExp.append(fieldName).append(":").append("[").append(value)
                     .append(" TO *]");
                solrQuery.setFilterQueries(fqExp.toString());
            }
            
            if (condition.getComparisonOp().equals("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan")
                    || condition.getComparisonOp().equals("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan")) {
                String fieldName = condition.getColumnDesc().getColumn();
                String value = condition.getConstantDesc().getValue().toString();
                // Formulating Filter Query Expression.
                StringBuffer fqExp = new StringBuffer();
                fqExp.append(fieldName).append(":").append("[* TO ").append(value)
                     .append(" ]");
                solrQuery.setFilterQueries(fqExp.toString());
            }
        }
        return solrQuery;
    }

}

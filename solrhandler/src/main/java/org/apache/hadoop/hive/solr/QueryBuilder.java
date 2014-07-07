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
        String query = job.get(ExternalTableProperties.SOLR_QUERY);
        SolrQuery solrQuery = new SolrQuery();
        String fields = StringUtils.join(ExternalTableProperties.COLUMN_NAMES, ", ");
        System.out.println("Fields -> " + fields);
        solrQuery.set("fl", fields);
        solrQuery.set("wt", "json");
        solrQuery.setQuery(query);
        // pass the filter query by doing predicate pushdown.
        String filterExprSerialized = job.get(TableScanDesc.FILTER_EXPR_CONF_STR);
        if(filterExprSerialized == null) return solrQuery;
        ExprNodeDesc filterExpr = Utilities.deserializeExpression(filterExprSerialized);
        IndexPredicateAnalyzer analyzer = PredicateAnalyzer.getPredicateAnalyzer();
        List<IndexSearchCondition> searchConditions = new ArrayList<IndexSearchCondition>();
        ExprNodeDesc residualPredicate = analyzer.analyzePredicate(filterExpr, searchConditions);
        for (IndexSearchCondition condition : searchConditions){
            
            if (condition.getComparisonOp().equals("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan")) {
                System.out.println("Greater Than Operator Found");
                String fieldName = condition.getColumnDesc().getColumn();
                System.out.println("Field Name ->" + fieldName);
                String value = condition.getConstantDesc().getValue().toString();
                System.out.println("Value " + value);
                // Formulating Filter Query Expression.
                StringBuffer fqExp = new StringBuffer();
                fqExp.append(fieldName).append(":").append("[").append(value)
                     .append(" TO *]");
                System.out.println(fqExp.toString());
                solrQuery.setFilterQueries(fqExp.toString());
            }
            
        }
        return solrQuery;
    }

}

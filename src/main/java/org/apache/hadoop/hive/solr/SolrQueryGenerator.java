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
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;

public class SolrQueryGenerator {

  private static final Logger LOG = Logger.getLogger(SolrQueryGenerator.class.getName());

  public static SolrQuery generateQuery(JobConf job){
    SolrQuery solrQuery = new SolrQuery();
    String query = job.get(ExternalTableProperties.SOLR_QUERY);
    solrQuery.setQuery(query);
    String fields = StringUtils.join(new ExternalTableProperties().COLUMN_NAMES, ", ");
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
    analyzer.analyzePredicate(filterExpr, searchConditions);
    for (IndexSearchCondition condition : searchConditions){

      String fieldName = condition.getColumnDesc().getColumn();
      String value = condition.getConstantDesc().getValue().toString();
      StringBuffer fqExp = new StringBuffer();
      if (condition.getComparisonOp().equals("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual")){
        // Formulating Filter Query Expression.
        fqExp.append(fieldName).append(":").append(value);
        solrQuery.addFilterQuery(fqExp.toString());
        LOG.debug("Equals comparison found, adding it to SOLR filter query");
      }
      if (condition.getComparisonOp().equals("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan")){
        fqExp.append(fieldName).append(":").append("{").append(value)
        .append(" TO *}");
        solrQuery.addFilterQuery(fqExp.toString());
        LOG.debug("Greater than comparison found, adding it to SOLR filter query");
      }

      if (condition.getComparisonOp().equals("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan")) {
        fqExp.append(fieldName).append(":").append("[").append(value)
        .append(" TO *]");
        solrQuery.addFilterQuery(fqExp.toString());
        LOG.debug("Greater than or equals comparison found, adding it to SOLR filter query");
      }

      if (condition.getComparisonOp().equals("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan")){
        fqExp.append(fieldName).append(":").append("{* TO ").append(value)
        .append(" }");
        solrQuery.addFilterQuery(fqExp.toString());
        LOG.debug("Less than comparison found, adding it to SOLR filter query");
      }

      if (condition.getComparisonOp().equals("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan")) {
        fqExp.append(fieldName).append(":").append("[* TO ").append(value)
        .append(" ]");
        solrQuery.addFilterQuery(fqExp.toString());
        LOG.debug("Less than or equals comparison found, adding it to SOLR filter query");
      }
    }
    return solrQuery;
  }
}

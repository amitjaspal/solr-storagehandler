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

import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.log4j.Logger;

/*
 * PredicateAnalyzer class exposes a custom predicate analyzer that is used by
 * the SolrStorageHandlert to analyze the predicates.
 */

public class PredicateAnalyzer {

  static final Logger LOG = Logger.getLogger(PredicateAnalyzer.class);
  /*
   * This method initializes the PredicateAnalyzer that is consumed
   * by the Hive query optimizer while evaluating the query.
   * We can plug in predicates to be pushed into SOLR in this method.
   * A know limitation of predicate pushdown is that only predicates with
   * conjunctions will be pushed to the SOLR level.
   */
  public static IndexPredicateAnalyzer getPredicateAnalyzer() {

    IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();
    analyzer.addComparisonOp(GenericUDFOPEqual.class.getName());
    analyzer.addComparisonOp(GenericUDFOPGreaterThan.class.getName());
    analyzer.addComparisonOp(GenericUDFOPEqualOrGreaterThan.class.getName());
    analyzer.addComparisonOp(GenericUDFOPLessThan.class.getName());
    analyzer.addComparisonOp(GenericUDFOPEqualOrLessThan.class.getName());
    //TODO: Add support for LIKE operator.
    return analyzer;
  }

}

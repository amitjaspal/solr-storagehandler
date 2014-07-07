package org.apache.hadoop.hive.solr;

import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;

public class PredicateAnalyzer {
    
    /*
     * This method initializes the PredicateAnalyzer that is consumed 
     * by the Hive query optimizer while evaluating the query.
     * We can plug in predicates to be pushed into SOLR in this method.
     * A know limitation of predicate pushdown is that only predicates with
     * conjunctions will be pushed at the SOLR level.
     */
    public static IndexPredicateAnalyzer getPredicateAnalyzer(){
        
        IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan");
        return analyzer;
    }

}
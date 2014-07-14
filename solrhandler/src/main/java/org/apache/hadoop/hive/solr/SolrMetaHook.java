package org.apache.hadoop.hive.solr;

import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

/*
 * SolrMetaHook module is responsible for making sure that SOLR backed
 * data store is in sync with the Hive meta-store.
 */
public class SolrMetaHook implements HiveMetaHook{
    
    public void preCreateTable(Table table) throws MetaException {
   
    }

    public void rollbackCreateTable(Table table) throws MetaException {
        
    }

    public void commitCreateTable(Table table) throws MetaException {
        
    }

    public void preDropTable(Table table) throws MetaException {

    }   
    
    public void rollbackDropTable(Table table) throws MetaException {
        
    }

    public void commitDropTable(Table table, boolean deleteData) throws MetaException {
        
    }
}

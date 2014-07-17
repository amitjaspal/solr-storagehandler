package org.apache.hadoop.hive.solr;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.solr.common.SolrInputDocument;

public class SolrRecordWriter implements RecordWriter{ 

    private SolrDAO solrDAO;
    
    public SolrRecordWriter(SolrDAO solrDAO){
        this.solrDAO = solrDAO;
    }
    
    @Override
    public void write(Writable wrt) throws IOException{
        MapWritable tuple = (MapWritable) wrt;
        SolrInputDocument doc = new SolrInputDocument();
        for(Map.Entry<Writable, Writable> entry:tuple.entrySet()){
            doc.setField(entry.getKey().toString(), entry.getValue().toString());
        }
        solrDAO.saveDoc(doc);
        return ;
    }
    
    
    @Override
    public void close(boolean f) throws IOException{
        solrDAO.commit();
    }
}
package org.apache.hadoop.hive.solr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.solr.common.SolrDocument;

public class SolrRecordReader implements RecordReader<LongWritable, MapWritable>{
    
    private SolrDAO solrDAO;
    private HiveSolrSplit hiveSolrSplit;
    private Integer currentPosition;
    
    public SolrRecordReader(InputSplit solrSplit, SolrDAO solrDAO){
        this.hiveSolrSplit = (HiveSolrSplit)solrSplit;
        this.currentPosition = 0;
        this.solrDAO = solrDAO;
    }
    
    @Override
    public void close() throws IOException{
    }
    
    @Override
    public LongWritable createKey(){
        return new LongWritable();
    }

    @Override
    public MapWritable createValue(){
        return new MapWritable();
    }
    
    @Override
    public long getPos() throws IOException{
        return this.currentPosition;
    }
    
    @Override
    public float getProgress() throws IOException{
        return currentPosition / solrDAO.getLength();
    }
    
    @Override
    public boolean next(LongWritable key, MapWritable value){
        SolrDocument doc = solrDAO.getNextDoc();
        if( doc == null) return false;
        key.set(currentPosition);
        
        for(String field : doc.getFieldNames()){
            String val = doc.getFieldValue(field).toString();
            value.put(new Text(field), new Text(val));
        }
        return true;
    }
    
}


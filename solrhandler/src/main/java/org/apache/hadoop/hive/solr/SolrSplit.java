package org.apache.hadoop.hive.solr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;

public class SolrSplit implements InputSplit{
    
    private String shardURL;
    
    SolrSplit(){
       
    }
    
    SolrSplit(String shardURL){
        this.shardURL = shardURL;
    }
    
    @Override
    public long getLength() throws IOException {
        return 0;
    }
    
    @Override
    public String[] getLocations() throws IOException {
        return new String[]{ };
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        shardURL = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(shardURL);
    }
}

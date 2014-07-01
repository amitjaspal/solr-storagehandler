package org.apache.hadoop.hive.solr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;


class HiveSolrSplit extends FileSplit {
    
    private InputSplit solrSplit;
    private Path path;
    
    public HiveSolrSplit(){
        //super(null, 0, 0, (String[]) null);
        this(new SolrSplit(), new Path("hdfs://localhost:8020/user/hive/warehouse/books"));
    }
    
    HiveSolrSplit(InputSplit solrSplit, Path path){
        super(path, 0, 0, (String[]) null);
        this.solrSplit = solrSplit;
        this.path = path;
    }
    
    public long getLength() {
        return 1l;
    }
    
    public String[] getLocations() throws IOException{
        return solrSplit.getLocations();
    }
    
    public void write(DataOutput out) throws IOException{
        solrSplit.write(out);
    }
    
    public void readFields(DataInput in) throws IOException {
        if(solrSplit == null){
            System.out.println("SolrSplit is null");
        }
        if(in == null){
            System.out.println("InputStream is null");
        }
        solrSplit.readFields(in);
    }

    @Override
    public String toString() {
        return solrSplit.toString();
    }

    @Override
    public Path getPath() {
        return path;
    }
    
}

package org.apache.hadoop.hive.solr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;

/*
 * HiveSolrSplit is just a wrapper class on top of SolrSplit. It seems 
 * Hive considers data sources to be of FileFormat so we need to define
 * the wrapper class HiveSolrSplit which extends from FileSplit.
 * Under the hood all functionalities are delegated to SolrSplit 
 * only.
 */
class HiveSolrSplit extends FileSplit {
    
    private SolrInputSplit solrSplit;
    private Path path;
    
    public HiveSolrSplit(){
       this(new SolrInputSplit(), new Path("hdfs://localhost:8020/user/hive/warehouse/"));
    }
    
    HiveSolrSplit(SolrInputSplit solrSplit, Path path){
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
        Text.writeString(out, path.toString());
        solrSplit.write(out);
    }
    
    public void readFields(DataInput in) throws IOException {
        path = new Path(Text.readString(in));
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
    
    public SolrInputSplit getSolrSplit(){
        return this.solrSplit;
    }
}

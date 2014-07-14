package org.apache.hadoop.hive.solr;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.Progressable;

import com.sun.rowset.internal.Row;



@SuppressWarnings("rawtypes")
public class SolrOutputFormat implements HiveOutputFormat<NullWritable, Row>
 {
    
    @Override
    public org.apache.hadoop.mapred.RecordWriter getRecordWriter(FileSystem ignored, JobConf job,
            String name, Progressable progress) throws IOException {
        // Hive will not call this method.
        return null;
    }
    
    @Override
    public RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
            final Class<? extends Writable> valueClass, boolean isCompressed,
            Properties tableProperties, Progressable progress) throws IOException{
        
        // Need to figure out how to improve the degree of parallelism.
        // For now we will just have 1 shard inserting all the documents.
        
        System.out.println("Returning record writer - ");
        String baseURL = "http://ubuntu:8983/solr";
        String shardName = "collection2_shard1_replica1";
        String collectionName = "collection2";
        SolrDAO solrDAO = new SolrDAO(baseURL, shardName, collectionName);
        return new SolrRecordWriter(solrDAO);
    }

    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf jc) throws IOException{
        /*System.out.println("Setting number of reduce jobs to 2");
        jc.setNumReduceTasks(2);
        Job job = new Job(jc);
        JobContext jobContext = ShimLoader.getHadoopShims().newJobContext(job); */
    }
    

}

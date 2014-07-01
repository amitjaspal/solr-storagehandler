package org.apache.hadoop.hive.solr;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

import com.sun.rowset.internal.Row;



@SuppressWarnings("rawtypes")
public class SolrOutputFormat implements HiveOutputFormat <NullWritable, Row>{
    
    public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException{
        
    }
    
    public org.apache.hadoop.mapred.RecordWriter getRecordWriter(FileSystem ignored, JobConf job,
            String name, Progressable progress) throws IOException {
        // Hive will not call this method.
        return null;
    }
    
    @Override
    public RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
            final Class<? extends Writable> valueClass, boolean isCompressed,
            Properties tableProperties, Progressable progress) throws IOException{
        
        SolrDAO solrDAO = new SolrDAO(jc.get(ExternalTableProperties.SOURCE_URL),
                                      jc.get(ExternalTableProperties.SOURCE_QUERY));
        return new SolrRecordWriter(solrDAO);
    }

    
    

}

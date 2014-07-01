package org.apache.hadoop.hive.solr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class SolrInputFormat implements InputFormat{
    
    @Override
    public FileSplit[] getSplits(JobConf job, int numSplits){
        
        Path path = new Path("hdfs://localhost:8020/user/hive/warehouse/books");
        FileSplit wrapper = new HiveSolrSplit(new SolrSplit(job.get(ExternalTableProperties.SOURCE_URL)), path);
        return new FileSplit[] {wrapper};
    }
    
    @Override
    public RecordReader<LongWritable, MapWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter){
        return new SolrRecordReader(split, new SolrDAO(job.get(ExternalTableProperties.SOURCE_URL),
                job.get(ExternalTableProperties.SOURCE_QUERY)));
    }

}

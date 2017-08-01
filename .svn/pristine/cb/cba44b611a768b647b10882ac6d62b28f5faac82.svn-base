package com.aotain.dw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CountDriver  extends Configured implements Tool{
	
	public int run(String[] arg) throws Exception {
        // TODO Auto-generated method stub
		//hbase
		if(arg.length != 6)
		{
			System.err.printf("Usage: %s <server><input><output><startkey><endkey><groupby>",getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;                  
		}
		
		String Server = arg[0];
		String InputTable = arg[1];
		String OutputTable = arg[2];
		String StartKey = arg[3];
		String EndKey = arg[4];
		String GroupBy = arg[5];
		
		
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", Server);  //Ç§Íò±ðÍü¼ÇÅäÖÃ
        conf.set("hbase.zookeeper.property.clientPort","2181");
        //conf.set("hbase.datetime", DateTime);
        conf.set("hbase.groupby", GroupBy);
        
        
	    @SuppressWarnings("deprecation")
        Job job = Job.getInstance(conf, "Count[" + InputTable 
        		+ "#From:" + StartKey + " To:" + EndKey + "]");
        job.setJarByClass(CountDriver.class);

       
        
        Scan scan = new Scan(StartKey.getBytes(), EndKey.getBytes());
        
        for(String col:GroupBy.split(","))
        {
        	scan.addColumn(Bytes.toBytes("cf"),Bytes.toBytes(col));
        }
        
        TableMapReduceUtil.initTableMapperJob(InputTable, 
				 scan,CountMapper.class,
				 Text.class, 
				 IntWritable.class, job);
        
        job.setMapperClass(CountMapper.class);
        job.setReducerClass(CountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        TableMapReduceUtil.initTableReducerJob(OutputTable, 
        		CountReducer.class,job);
        
        job.waitForCompletion(true);
        return 0;
    }
	
	
	public static void main(String [] args) throws Exception{
        int mr;
        mr = ToolRunner.run(new Configuration(),new CountDriver(),args);
        System.exit(mr);
    }
}

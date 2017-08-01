package com.aotain.dw;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UserMailDriver extends Configured implements Tool
{
	public static void main(String[] args) throws Exception 
	{
        int mr;
        mr = ToolRunner.run(new Configuration(),new UserMailDriver(),args);
        System.exit(mr);
	}
	
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum","172.16.1.40");
		conf = HBaseConfiguration.create(conf);
		
        //用户列表容器
		HashSet<String> set_userlist = new HashSet<String>(); 
    	
    	//获取用户列表
    	HTable t_userlist = new HTable(conf,"userlist");
    	Scan scan = new Scan();
    	ResultScanner rs = t_userlist.getScanner(scan);
    	for (Result r : rs) 
    	{  
    		for (Cell cell : r.listCells()) 
    		{  
            	 set_userlist.add("foshan_"+Bytes.toString(CellUtil.cloneValue(cell)));
    		}
    	}
        
    	System.out.println("userlist size: "+set_userlist.size());
    	
        //设置参数
    	DefaultStringifier.store(conf, set_userlist ,"userlist");
		
		Job job = Job.getInstance(conf, "getusermail");
		job.setJarByClass(UserMailDriver.class);

		List<Filter> filters = new ArrayList<Filter>();
		//过滤地市
		SingleColumnValueFilter filter = new SingleColumnValueFilter(  
			    Bytes.toBytes("USERINFO"),  
			    Bytes.toBytes("location"),  
			    CompareOp.EQUAL,  
			    Bytes.toBytes("foshan"));
		filter.setFilterIfMissing(true); 
		scan.setFilter(filter);   
		
		TableMapReduceUtil.initTableMapperJob("usermails", 
				scan,UserMailMapper.class,
				Text.class, IntWritable.class,job);
				 
		job.setReducerClass(UserMailReduceer.class);
	     
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
}

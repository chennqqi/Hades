package com.aotain.hbase.dataimport;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HbaseImport {

	public static long startTime; 
    public static void main(String[] args) throws IOException  {

	
	System.out.println("start time = " + 111111111);
	insert_one(args[0],"");

    }

	public static void insert_one(String path,String tablename) throws IOException {
    System.out.println("=============start conf");
	Configuration conf = HBaseConfiguration.create();
	conf.set("hbase.zookeeper.property.clientPort","2181");
	conf.set("hbase.zookeeper.qu orum", "172.16.1.34");  
	conf.set("hbase.master", "172.16.1.40:600000");
	 System.out.println("=============end conf");
	HTable table = new HTable(conf,"cu_snap");
	System.out.println("=============end conf  table");
	File f = new File(path);

	ArrayList<Put> list = new ArrayList<Put>();

	BufferedReader br = new BufferedReader(new FileReader(f));

	String tmp =br.readLine();

	int count = 0;
	
	while (tmp  != null) {
		if (list.size() > 10000) {

			table.put(list);

			table.flushCommits();
			list.clear();

			}

	
	String arr_value[] = tmp.toString().split("@", 3);
    if(arr_value.length!=3)
    	return ;
	String rowkey = arr_value[0];
	if(rowkey==null)
		return;
	String name =arr_value[1];

	String content = arr_value[2];

    Put p = new Put(rowkey.getBytes());

	p.add(("snap").getBytes(), "name".getBytes(),

	name.getBytes());

	p.add(("snap").getBytes(), "content".getBytes(),

			content.getBytes());

	

	list.add(p);

	

	tmp = br.readLine();

	count++;

	}

	if (list.size() > 0) {

	table.put(list);

	table.flushCommits();

	}
	System.out.println("=============put end");
    br.close();
	table.close();

	System.out.println("total = " + count);

	


	}
}

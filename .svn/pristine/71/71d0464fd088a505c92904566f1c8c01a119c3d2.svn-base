package com.aotain.hbase.dataoutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;



@SuppressWarnings("deprecation")
public class HBaseTestCase {
	
	static Configuration cfg = null;

	//static Logger log = Logger.getLogger(HBaseTestCase.class);
	
	static {
		Configuration HBASE_CONFIG = new Configuration();
		HBASE_CONFIG.set("hbase.zookeeper.quorum","shanghai-cm-1");
		HBASE_CONFIG.set("hbase.zookeeper.property.clientPort","2181");
		//0.96 特
		cfg = HBaseConfiguration.create(HBASE_CONFIG);
		
		//BasicConfigurator.configure();
		
		
	}
	
	
	@SuppressWarnings("resource")
	public static void getAllData(String tablename) throws Exception{
		HTable table = new HTable(cfg,tablename);
		Scan s = new Scan();
		
		s.addColumn("cf".getBytes(), "OSVersion".getBytes());
		
		ResultScanner ss = table.getScanner(s);
		int count = 0;
		HTable intable = new HTable(cfg,"intest");
		
		for (Result r : ss)
		{//行
			if(count>1000)
				return;
			for(Cell cell : r.listCells())
			{//列
				Put row = new Put(r.getRow());
				
				String famliy = Bytes.toString(CellUtil.cloneFamily(cell));
				
				String Column = Bytes.toString(CellUtil.cloneQualifier(cell));//列名
				
				String Value = Bytes.toString(CellUtil.cloneValue(cell));//值
				
				row.add(famliy.getBytes(),Column.getBytes(),Value.getBytes());
				
				intable.put(row);
				
				System.out.println(famliy + ":" +Column + "=" + Value);
			}
			
			count++;
		}
	}
	
	public static void getDataByKey(String tablename,String rowkey) throws Exception
	{
		HTable table = new HTable(cfg,tablename);
		    
		try {
			
			
		   Get get = new Get(rowkey.getBytes()); //根据主键查询
		   Result r = table.get(get);
		   System.out.println("start===showOneRecordByRowKey==row: "+"\n");
		   System.out.println("row: "+new String(r.getRow(),"utf-8"));
		   
		   for(KeyValue c : r.list()){
		    //时间戳转换成日期格式
		    String timestampFormat
		    	= new SimpleDateFormat("yyyy-MM-dd HH:MM:ss")
		    .format(new Date(c.getTimestamp()));
		       //System.out.println("===:"+timestampFormat+"  ==timestamp: "+kv.getTimestamp());
		    System.out.println("\nKeyValue: "+c);
		    //System.out.println("key: "+kv.getKeyString());
		    
		    System.out.println("family=>"+Bytes.toString(c.getFamily())
		          +"  value=>"+Bytes.toString(c.getValue())
		    +"  qualifer=>"+Bytes.toString(c.getQualifier())
		    +"  timestamp=>"+timestampFormat);
		 
		   }
		  } catch (IOException e) {
		   // TODO Auto-generated catch block
		   e.printStackTrace();
		  }
		  System.out.println("end===========showOneRecordByRowKey");
	}
	
	/**
	 * 根据rowkey的范围查询
	 * @param tablename
	 * @param startkey
	 * @param endkey
	 * @throws Exception
	 */
	public static void getData(String tablename,String columns[],
			String startkey,String endkey) throws Exception
	{
		HTable table = new HTable(cfg,tablename);
		int rownum = 0;
		try {
			
			Scan sc = new Scan(startkey.getBytes(), endkey.getBytes());
			String line = "rowkey,";
			for(String col : columns)
			{
				sc.addColumn(Bytes.toBytes("cf"),Bytes.toBytes(col));
				line = line + col + ",";
			}
			System.out.println(tablename + " start===row: "+"\n");
			
			System.out.println(line);
			ResultScanner ss = table.getScanner(sc);
		    
		   	for (Result r : ss)
			{//行
		   		line = Bytes.toString(r.getRow()) + ",";
		   		rownum++;
		   		for(Cell c : r.listCells()){
				   //时间戳转换成日期格式
				   
				   line = line +  Bytes.toString(CellUtil.cloneValue(c)) + ",";
		   		}
		   		System.out.println(line);
			 }
		  } catch (IOException e) {
		   // TODO Auto-generated catch block
		   e.printStackTrace();
		  }
		System.out.println("end===row:" + rownum);
	}
	
	
	 public static void QueryByCondition2(String tablename,String column1,String value1) { 
	        try { 
	        	HTable table = new HTable(cfg,tablename);
	            Filter filter = new SingleColumnValueFilter(Bytes.toBytes(column1), null, CompareOp.EQUAL, Bytes 
	                    .toBytes(value1)); // 当列column1的值为aaa时进行查询 
	            Scan s = new Scan(); 
	            s.setFilter(filter); 
	            ResultScanner rs = table.getScanner(s); 
	            for (Result r : rs) { 
	                System.out.println("获获得rowkey:" + new String(r.getRow())); 
	                for (KeyValue c : r.list()) { 
	                    System.out.println("列：" + Bytes.toString(c.getQualifier()) 
	                            +" ====值:" + Bytes.toString(c.getValue())); 
	                } 
	            } 
	        } catch (Exception e) { 

	            e.printStackTrace(); 

	        } 

	 

	    } 

	 

	     

	    public static void QueryByCondition3(String tablename,
	    		String column1,String value1,
	    		String column2,String value2,
	    		String column3,String value3) { 

	 

	        try { 

	        	HTable table = new HTable(cfg,tablename);

	            List<Filter> filters = new ArrayList<Filter>(); 

	 

	            Filter filter1 = new SingleColumnValueFilter(Bytes 

	                    .toBytes(column1), null, CompareOp.EQUAL, Bytes 

	                    .toBytes(value1)); 

	            filters.add(filter1); 

	 

	            Filter filter2 = new SingleColumnValueFilter(Bytes 

	                    .toBytes(column2), null, CompareOp.EQUAL, Bytes 

	                    .toBytes(value2)); 

	            filters.add(filter2); 

	 

	            Filter filter3 = new SingleColumnValueFilter(Bytes 

	                    .toBytes(column3), null, CompareOp.EQUAL, Bytes 

	                    .toBytes(value3)); 

	            filters.add(filter3); 

	 

	            FilterList filterList1 = new FilterList(filters); 

	 

	            Scan scan = new Scan(); 

	            scan.setFilter(filterList1); 

	            ResultScanner rs = table.getScanner(scan); 

	            for (Result rr : rs) { 

	                System.out.println("获获得rowkey: " + new String(rr.getRow())); 

	                for (KeyValue c : rr.list()) { 

	                    System.out.println("列： "+ Bytes.toString(c.getQualifier())

	                            +"====值:" + Bytes.toString(c.getValue())); 
	                } 

	            } 

	            rs.close(); 

	 

	        } catch (Exception e) { 

	            e.printStackTrace(); 

	        } 

	 

	    } 
	
	public static void main(String[] args){
		try{
			String tablename = args[0];
			String startkey = args[1];
			String endkey = args[2];
			System.out.println("START EXPORT");
			HBaseTestCase.getData(tablename,new String[] {"PV","InfoValue"},
					startkey,endkey);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	 public static int generateRandom(int a, int b) {
	        int temp = 0;
	        try {
	            if (a > b) {
	                temp = new Random().nextInt(a - b);
	                return temp + b;
	            } else {
	                temp = new Random().nextInt(b - a);
	                return temp + a;
	            }
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	        return temp + a;
	 }
	 
}

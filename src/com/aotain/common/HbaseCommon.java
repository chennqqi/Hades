package com.aotain.common;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;



public class HbaseCommon {
	static Configuration cfg = null;
	
	private static Connection connection = null;
	

	
	private static void InitConnection()
	{
		Configuration HBASE_CONFIG = HBaseConfiguration.create();
		
		HBASE_CONFIG.set("hbase.zookeeper.quorum", "192.168.5.95");
		//HBASE_CONFIG.set("hbase.zookeeper.quorum", "shanghai-cm-5");  //千万别忘记配置
		HBASE_CONFIG.set("hbase.zookeeper.property.clientPort","2181");
		//HBASE_CONFIG.set("fs.defaultFS","hdfs://hive-2:8020");
		//HBASE_CONFIG.set("zookeeper.znode.parent","/user/hbase");
		//0.96 特
		//System.setProperty("hadoop.home.dir", "C:\\cygwin\\usr\\hadoop");
		cfg = HBaseConfiguration.create(HBASE_CONFIG);
		try {
			connection = ConnectionFactory.createConnection(HBASE_CONFIG);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
		System.out.println("IP:"+cfg.get("hbase.zookeeper.quorum"));
		System.out.println("PORT:"+cfg.get("hbase.zookeeper.property.clientPort"));
	}
	
	
	@SuppressWarnings("resource")
	public static void getAllData(String tablename) throws Exception{
		
		//HBaseAdmin admin =new HBaseAdmin(cfg);  
        //if (admin.tableExists(tablename)) {  
         //   System.out.println("表已经存在！");  
        //}
		HTable table = new HTable(cfg,tablename);
		Scan s = new Scan();
		ResultScanner ss = table.getScanner(s);
		

        
		for (Result r : ss)
		{//行
			String rowkey = Bytes.toString(r.getRow());
			for(Cell c : r.listCells())
			{//列
				
				String ff = Bytes.toString(CellUtil.cloneFamily(c));//列簇
				String cc = Bytes.toString(CellUtil.cloneQualifier(c));//
				
				
				
				if(cc.equals("PV"))
				{
					Long bb = Bytes.toLong(CellUtil.cloneValue(c));//值
					System.out.println(rowkey + "/" + ff + ":" + cc + "=" + bb);
					
				}
				else
				{
					String bb = Bytes.toString(CellUtil.cloneValue(c));
					System.out.println(rowkey + "/" + ff + ":" + cc + "=" + bb);
				}
				
				
			}
			
			/*
			for(KeyValue kv:r.raw()){
				//System.out.println(kv.getRowLength());
				String ff = new String(kv.getFamily(),"UTF-8");//列名
				//String aa = kv.keyToString(kv.getKey());
				String bb = new String(kv.getValue(),"UTF-8");//值
				
				System.out.println(ff + ":" + bb);
				
				
				//System.out.print(new String(kv.getValue()));
			}*/
		}
	}
	
	@SuppressWarnings("resource")
	public static void GetDataToFile(String tablename,String startKey,
			String endKey,String[] columns,String filepath) throws Exception{

        HTable table = new HTable(cfg,tablename);
        Scan sc = new Scan(startKey.getBytes(), endKey.getBytes());
        for(String col:columns)
        {
        	sc.addColumn(Bytes.toBytes("cf"),Bytes.toBytes(col));
        }
		ResultScanner ss = table.getScanner(sc);
		int count = 0;
        FileWriter fw = null;   
	    try {   
	        fw = new FileWriter(filepath);   
	        for (Result r : ss)
	    	{//行
	    		for(Cell c : r.listCells())
	    		{//列
	    			String value = Bytes.toString(CellUtil.cloneValue(c));//值
	    			fw.write(value + ",");  
	    		}
	    		fw.write("\r\n"); 
	    		count++;
	    	}
	        fw.close();   

	    } catch (Exception e) {   
	        e.printStackTrace();   
	    }
	    finally {   
	       try {   
	    	   fw.close();   
	       } catch (Exception e) {   
	            e.printStackTrace();   
	       }
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
		   
		   for(Cell c : r.listCells()){
		    //时间戳转换成日期格式
		    String timestampFormat
		    	= new SimpleDateFormat("yyyy-MM-dd HH:MM:ss")
		    .format(new Date(c.getTimestamp()));
		       //System.out.println("===:"+timestampFormat+"  ==timestamp: "+kv.getTimestamp());
		    System.out.println("\nKeyValue: "+c);
		    //System.out.println("key: "+kv.getKeyString());
		    
		    System.out.println("family=>"+Bytes.toString(CellUtil.cloneFamily(c))
		          +"  value=>"+Bytes.toString(CellUtil.cloneValue(c))
		    +"  qualifer=>"+Bytes.toString(CellUtil.cloneValue(c))		    
		    +"  timestamp=>"+timestampFormat);
		 
		   }
		  } catch (IOException e) {
		   // TODO Auto-generated catch block
		   e.printStackTrace();
		  }
		  System.out.println("end===========showOneRecordByRowKey");
	}
	
	
	public static void AddData(String tablename,String rowkey,String family,String column,String value)
	{
		
		
		try {
			HTable table = new HTable(cfg,tablename);
			Put p = new Put(rowkey.getBytes());
			p.add(family.getBytes(),column.getBytes(),value.getBytes("UTF8"));
			table.put(p);
		}  catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
	/**
	 * 根据rowkey的范围查询
	 * @param tablename
	 * @param startkey
	 * @param endkey
	 * @throws Exception
	 */
	public static void getData(String tablename,String startkey,String endkey,int limit) throws Exception
	{
		
		InitConnection();
		
		TableName TABLE_NAME = TableName.valueOf(tablename);
		Table table = connection.getTable(TABLE_NAME);
		  
		//HTable table = new HTable(cfg,tablename);
		int rownum = 0;
		try {
			
		   Scan sc = new Scan(startkey.getBytes(), endkey.getBytes());
		   sc.setReversed(true);
		   sc.setCaching(10);
		   Filter filter = new PageFilter(limit);
		   sc.setFilter(filter);  
		   
		   ResultScanner ss = table.getScanner(sc);
		   System.out.println("start===showOneRecordByRowKey==row: "+"\n");
		   
		   for (Result r : ss)
			{//行
			   String rowkey = Bytes.toString(r.getRow());
			   String row = "";
			   rownum++;
			   row = rowkey + "/";
			   for(Cell c : r.listCells()){
				   String ff = Bytes.toString(CellUtil.cloneFamily(c));//列簇
				   String cc = Bytes.toString(CellUtil.cloneQualifier(c));//

				   if(cc.contains("LOW") || cc.contains("MIDDLE")
						   || cc.equals("PV"))
				   {
					   Long bb = Bytes.toLong(CellUtil.cloneValue(c));//值
					   //System.out.print(ff + ":" + cc + "=" + bb + "/");
					   row = row + ff + ":" + cc + "=" + bb + "/";
				   }
				   else
				   {
					   String bb = Bytes.toString(CellUtil.cloneValue(c));
					   //System.out.print(ff + ":" + cc + "=" + bb + "/");
					   row = row + ff + ":" + cc + "=" + bb + "/";
				   }
			   }
		   
			   //if(rownum <= limit)
			   {
				   System.out.print(row);
				   System.out.println();
			   }
			   //else
			   {
				//   break;
			   }
			}
		  } catch (IOException e) {
		   // TODO Auto-generated catch block
			  e.printStackTrace();
		  }
		finally
		{
			table.close();
			connection.close();
		}
		System.out.println("end===========row:" + rownum);
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
	                for (Cell c : r.listCells()) { 
	                    System.out.println("列：" 
	                + Bytes.toString(CellUtil.cloneQualifier(c)) 
	                +" ====值:" + Bytes.toString(CellUtil.cloneValue(c))); 
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

	                for (Cell c : rr.listCells()) { 

	                    System.out.println("列： "+ Bytes.toString(CellUtil.cloneQualifier(c))

	                            +"====值:" + Bytes.toString(CellUtil.cloneValue(c))); 
	                } 

	            } 

	            rs.close(); 

	 

	        } catch (Exception e) { 

	            e.printStackTrace(); 

	        } 

	 

	    } 
	    
	 public static void createTable(String tableName) {  
	        System.out.println("start create table ......");  
	        try {  
	            HBaseAdmin hBaseAdmin = new HBaseAdmin(cfg);  
	            if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建  
	            	System.out.println(tableName + " is exist");
	            	return;
	            	//hBaseAdmin.disableTable(tableName);  
	                //hBaseAdmin.deleteTable(tableName);  
	                //System.out.println(tableName + " is exist,detele....");  
	            }  
	            
	            //HashChoreWoker worker = new HashChoreWoker(1000000, 100);  
	            //byte[][] splitKeys = worker.calcSplitKeys();  
	            
	            HTableDescriptor tableDescriptor 
	            	=  new HTableDescriptor(TableName.valueOf(tableName));  
	            HColumnDescriptor cf = new HColumnDescriptor("cf");
	          
	            cf.setCompressionType(Algorithm.SNAPPY);
	            cf.setMaxVersions(1);
	            
	            tableDescriptor.addFamily(cf);
	            //tableDescriptor.setCompactionEnabled(true);
	            //tableDescriptor.setConfiguration("COMPRESSION", "snappy");
	            
	            
	            hBaseAdmin.createTable(tableDescriptor); 
	            
	        } catch (MasterNotRunningException e) {  
	            e.printStackTrace();  
	        } catch (ZooKeeperConnectionException e) {  
	            e.printStackTrace();  
	        } catch (IOException e) {  
	            e.printStackTrace();  
	        }  
	        System.out.println("end create table ......");  
	    }   
	
	public static void main(String[] args){
		try{
			//String tablename="SDS_ABNORMAL_LOG";
			//String tablename="SDS_IDC_LOG";
			//String tablename = args[0];
			//String startrow = args[1];
			//String endrow =  args[2];
//			
//			String driverServer = "shanghai-cm-8:9528";
//			String server = driverServer.split(":",-1)[0];
//			int port = Integer.parseInt(driverServer.split(":",-1)[1]);
//			System.out.println(driverServer);  
//			System.out.println(server);  
//			System.out.println(port);  
			
			while(true)
			{
			
				BufferedReader strin=new BufferedReader(new InputStreamReader(System.in));  
	            System.out.print("TABLENAME：");  
	            String tablename = strin.readLine();  
	              
	              
	            System.out.println("STARTROW：");  
	            String startrow = strin.readLine();  
	            
	            System.out.println("ENDROW：");  
	            String endrow = strin.readLine();  
	            
	
				HbaseCommon.getData(tablename, startrow, endrow,200);
				
				//HbaseCommon.getData(tablename, "101.227.160.27_2015092700", "101.227.160.27_2015092300");
				//HbaseCommon.getData(tablename, "101.227.130.26_2015092300", "101.227.130.26_2015092400");
				//HbaseCommon.getAllData(tablename);
				System.out.println("test");
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}

package com.aotain.project.apollo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.aotain.hbase.dataimport.HBaseRecordAdd;



/**
 * 网站评估
 * @author Administrator
 *
 */
public class SiteEvaluateMain {
	
	static SiteEvaluateMain singleton;

	private Configuration cfg = null;
	private Connection connection = null;
	String zooServer = "";
	String driverServer = "";
	
	static Object locker = new Object();
	 
	public SiteEvaluateMain(String zoo,String driver)
	{
		//初始化HBASE连接
		Configuration HBASE_CONFIG = HBaseConfiguration.create();
		HBASE_CONFIG.set("hbase.zookeeper.quorum", zoo);  //千万别忘记配置
		HBASE_CONFIG.set("hbase.zookeeper.property.clientPort","2181");	
		cfg = HBaseConfiguration.create(HBASE_CONFIG);
		try {
			connection = ConnectionFactory.createConnection(HBASE_CONFIG);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		zooServer = zoo;
		driverServer = driver;
		 
	}
	
	 public static SiteEvaluateMain getInstance(String zooServer,String driverServer) {
		  
		  if (singleton == null) {
		      synchronized (locker) {
		    	  if (singleton == null) {
		          singleton = new SiteEvaluateMain(zooServer,driverServer);
		    	  }
		      }
		  }
		  return singleton;
	  }
	
	public static void main(String[] args){
		try{
			//String tablename="SDS_ABNORMAL_LOG";
			//String tablename="SDS_IDC_LOG";
			String tablename = args[0];
			String startrow = args[1];
			String endrow =  args[2];
			
			//读取数据库表，获得需要进行评估的IP地址，按照IP地址循环评估网站得分
			
			

			//HbaseCommon.getData(tablename, startrow, endrow,10000);
			
			//HbaseCommon.getData(tablename, "101.227.160.27_2015092700", "101.227.160.27_2015092300");
			//HbaseCommon.getData(tablename, "101.227.130.26_2015092300", "101.227.130.26_2015092400");
			//HbaseCommon.getAllData(tablename);
			System.out.println("test");
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void EvaluateFunction(String IP,String StartDate,String EndDate)
	{
		Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(String.format("%s_%s", IP, StartDate)));
        scan.setStopRow(Bytes.toBytes(String.format("%s_%s", IP, EndDate)));
        scan.addFamily(Bytes.toBytes("cf"));
        scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("PORTLOW"));
        scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("SESSIONMIDDLE"));
        
        //需要读取的hbase表名
        String tableName = "SDS_ABN_SESS_STAT_H";
        
        TableName TABLE_NAME = TableName.valueOf(tableName);
		try {
			Table table = connection.getTable(TABLE_NAME);
			
			ResultScanner ss = table.getScanner(scan);
			double score = 0;
			int rownum = 0;
			for (Result r : ss)
			{//行
				
				long nPortPV = 0;
           		long nSessionPV = 0;
			   
			
           		//Cell destIP = r.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("IP"));
           		Cell portPV = r.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("PORTLOW"));
              
           		if(portPV != null)
               		nPortPV = Bytes.toLong(CellUtil.cloneValue(portPV));
               
           		Cell sessionPV = r.getColumnLatestCell(
                       Bytes.toBytes("cf"), Bytes.toBytes("SESSIONMIDDLE"));
               if(sessionPV != null)
               		nSessionPV = Bytes.toLong(CellUtil.cloneValue(sessionPV));
               rownum++;
               score = (nPortPV>0?70:100)*0.3 + (nSessionPV>0?60:100)*0.7 + score;
			}
			
			if(rownum == 0)
				score = 100;
			else
				score = score/rownum;
			
			HBaseRecordAdd hbaseInstance = HBaseRecordAdd.getInstance(
					zooServer,driverServer);
			
        	String destip = IP;
        	
			
			String rowkey = String.format("%s_%s", destip, StartDate);
			
			String tbName = "SDS_EVALUATE_SITE";
			
			hbaseInstance.Add(tbName, rowkey, "cf",  "IP", destip);
			hbaseInstance.Add(tbName, rowkey, "cf",  "REPORTTIME", StartDate);
			hbaseInstance.Add(tbName, rowkey, "cf", "EVALUATE", String.valueOf(score));
			
			//hbaseInstance.ImmdiateFlashData();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		

	}
}

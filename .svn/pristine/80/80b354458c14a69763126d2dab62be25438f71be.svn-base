package com.aotain.hbase.dataimport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.aotain.hbase.dataimport.CounterMap.Counter;
import com.aotain.mushroom.Slave;


public class HBaseRecordAdd {

	static HBaseRecordAdd singleton;
	
	//static String tableName;
	//static String columnFamily;
	//Table hTable;
	//HTable hTableNew;
	static long lastUsed;
	static long flushInterval = 60000;
	static CloserThread closerThread;
	static FlushThread flushThread;
	static HashMap<String, HBaseTable> tableRecordMap = null;
	
	private static String _zooServer;
	private static String _driverServer;
	
	private static Connection connection = null;
	
	private static HashMap<String,Table> tablePool = new HashMap<String, Table>();
	
	//private static Connection connectionStatic = null;
	
	  static Object locker = new Object();

	  public HBaseRecordAdd(String zooServer,String driverServer) {
		 //HBaseRecordAdd.tableName = tableName;
		 //HBaseRecordAdd.columnFamily = columnFamily;
		  _zooServer = zooServer;
		  _driverServer = driverServer;
		  initialize();
	  }
	  
	  public static HBaseRecordAdd getInstance(String zooServer,String driverServer) {
  
		  if (singleton == null) {
		      synchronized (locker) {
		    	  if (singleton == null) {
		          singleton = new HBaseRecordAdd(zooServer,driverServer);
		    	  }
		      }
		  }
		  return singleton;
	  }
	  
	private static void initialize() {
		 
		  if (tableRecordMap == null) {
			  Logger.getRootLogger().info("Init hbase conn*******************");
	          Configuration hConfig = HBaseConfiguration.create();
	          hConfig.set("hbase.zookeeper.quorum", _zooServer);
	          hConfig.set("hbase.zookeeper.property.clientPort","2181");  
	          updateLastUsed();
	          
	          try {
	            // establish the connection to the cluster.
	        	//TableName TABLE_NAME = TableName.valueOf(tableName);
	            connection = ConnectionFactory.createConnection(hConfig);
	            // retrieve a handle to the target table.
	            //hTable = connection.getTable(TABLE_NAME);
	            // describe the data we want to write.
	            //Put p = new Put(Bytes.toBytes("someRow"));
	            //p.addColumn(CF, Bytes.toBytes("qual"), Bytes.toBytes(42.0d));
	            // send the data.
	            //table.put(p);
	            tableRecordMap = new HashMap<String, HBaseTable>(); 
	          
	          } catch (IOException e) {
	            throw new RuntimeException(e);
	          }
	          flushThread = new FlushThread(flushInterval);
	          flushThread.start();
	          
	          System.out.println("Flush Thread Start");
	          
	          closerThread = new CloserThread();
	          closerThread.start();
	          
	          
		  }
	  }
	  
	  
	  private static void updateLastUsed() {
		    lastUsed = System.currentTimeMillis();
		  }
	  
	  /**
		 * 通过rowkey columnkey 获取cell值
		 * @param tablename
		 * @param rowkey
		 * @param columnKey
		 * @return
		 * @throws Exception
		 */
		public String getDataByKey(String tablename,String rowkey,String columnKey) throws Exception
		{
			/*
			try {
				//HTable table = new HTable(cfg,tablename);
				
			   Get get = new Get(rowkey.getBytes()); //根据主键查询
			   Result r = hTableNew.get(get);
			 
			   List<Cell> cells = r.getColumnCells(Bytes.toBytes("cf"), Bytes.toBytes(columnKey));
			   byte[] value = null;
			   for(Cell c : cells)
			   {
				   value = CellUtil.cloneValue(c);
			   }
			   String str = "";
			   if(value!=null)
				   str = new String(value);
			   return str;
			   
			  } catch (IOException e) {
			   // TODO Auto-generated catch block
				  e.printStackTrace();
			  }*/
			return "";
		}
		
	  protected void close() {
		    if (tablePool.size() > 0) {
		      synchronized (locker) {
		    	  for(Table hTable : tablePool.values())
		    	  {
		    		  if (hTable != null) {
		    			  if (hTable != null && System.currentTimeMillis() - lastUsed > 300000) {
		    				  flushThread.stopLoop();
		    				  flushThread = null;
		    				  try {
		    					  hTable.close();
		    				  } catch (IOException e) {
		    					  // TODO Auto-generated catch block
		    					  e.printStackTrace();
		    				  }
		    				  hTable = null;
		    			  	}
		    		  	}
		    		  if(tableRecordMap!=null)
		    		  {
			    		  synchronized (tableRecordMap) {
				    		  if(tableRecordMap != null)
				    		  {
				    			  tableRecordMap.clear();
				    			  tableRecordMap = null;
				    		  }
			    		  }
		    		  }
		    	  	}
		    	  if(connection !=null)
					try {
						connection.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
		      	}
		    }
	}
	  
	  /**
	   * PUT Add Record
	   * @param tableName
	   * @param rowKey
	   * @param columnFamily
	   * @param key
	   * @param value
	   */
	  public void Add(String tableName,
			  String rowKey, 
			  String columnFamily,
			  String key, 
			  String value) {
		  
		  if(tableRecordMap == null)
			  initialize();
		  
		  synchronized (tableRecordMap)
		  {
			  HBaseTable tbl = tableRecordMap.get(tableName);
			  
			  if(tbl == null)
			  {
				  tbl = new HBaseTable(tableName);
				  tableRecordMap.put(tableName, tbl);
			  }
			  
			  tbl.Add(rowKey, columnFamily, key, value);
			  
//			  Logger.getRootLogger().info(String.format("Add %s/%s/%s/%s/%s", 
//					  tableName,rowKey,columnFamily,key,value));
		  }
		  initialize();
	  }
	 
	  public void ImmdiateFlashData()
	  {
		  try {
			flushThread.flushToHBase();
			//close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	  }
	  
	  
	  /**
	   * 数值增量计算存储
	   * @param tableName
	   * @param rowKey
	   * @param key cf+column
	   * @param increment
	   */
	  public void incerment(String tableName, String rowKey, 
			  String key, int increment) {
		  synchronized (tableRecordMap)
		  {
			  HBaseTable tbl = tableRecordMap.get(tableName);
			  
			  if(tbl == null)
			  {
				  tbl = new HBaseTable(tableName);
				  tableRecordMap.put(tableName, tbl);
			  }
			  
			  tbl.incerment(rowKey, key, (long)increment);
		  }
		  initialize();
	  }
	  
	  public static class CloserThread extends Thread {

		  boolean continueLoop = true;
		  @Override
		  public void run() {
			  while (continueLoop) {
				  if (System.currentTimeMillis() - lastUsed > 300000) {
					  singleton.close();
					  break;
				  }

		        try {
		          Thread.sleep(60000);
		        } catch (InterruptedException e) {
		          e.printStackTrace();
		        }
		      }
		    }

		    public void stopLoop() {
		      continueLoop = false;
		    }
		  }

	  protected static class FlushThread extends Thread {
		  long sleepTime;
		  boolean continueLoop = true;

		  public FlushThread(long sleepTime) {
			  this.sleepTime = sleepTime;
		  }

		  @Override
		  public void run() {
			  Logger.getRootLogger().info("FlushThread Run..........*******************");
			  while (continueLoop) {
				  try {
					  Logger.getRootLogger().info("flushToHBase*******************");
					  flushToHBase();
				  } catch (Exception e) {
					  Logger.getRootLogger().error("flushToHBase ERROR:*********" + e.getMessage(),e);
					  break;
				  }

				  try {
					  Thread.sleep(sleepTime);
				  } catch (InterruptedException e) {
					  Logger.getRootLogger().error("FlushThread ERROR:*********" + e.getMessage(),e);
				  }
			  }
		  }
	  
		  public void flushToHBase() throws IOException {
			  synchronized (tableRecordMap) {
				  if (tableRecordMap == null) {
					  initialize();
				  }
				  updateLastUsed();
				 
				  synchronized (tableRecordMap) {
					  for (Entry<String, HBaseTable> entry : tableRecordMap.entrySet()) {
						  
						  synchronized (locker) {
							  int addCount = 0;
							  int increCount = 0;
							  String tableName = entry.getKey();
							  Logger.getRootLogger().info("###HBASE TABLENAME:" + tableName);
							  Table hTable = tablePool.get(tableName);
							  if(hTable == null)
							  {
								  TableName TABLE_NAME = TableName.valueOf(tableName);
								  hTable = connection.getTable(TABLE_NAME);
								  tablePool.put(tableName, hTable);
							  }
							  
							 //hTable.setAutoFlush(false);  
							  hTable.setWriteBufferSize(4*1024*1024);
							  HBaseTable rMap = entry.getValue();
							  
							  HashMap<String,ArrayList<HKeyValue>> rows = rMap.getRow();
							  //Logger.getRootLogger().info("Insert Table START " + tableName 
								//	  + ":" + rows.entrySet().size());
							  for (Entry<String, ArrayList<HKeyValue>> row : rows.entrySet()) {
								  try
								  {
									  Put put = new Put(Bytes.toBytes(row.getKey()));
									  
									  for(HKeyValue kv : row.getValue())
									  {
										  put.addColumn(Bytes.toBytes(kv.getColumnFamily()),
												  Bytes.toBytes(kv.getKey()), 
												  Bytes.toBytes(kv.getValue()));
										  //list.add(put);
//										  Logger.getRootLogger().info("###HBASE put ROWKEY:" 
//										  + row.getKey() + "/" +  kv.getKey() + "/" + kv.getValue());
									  }
									  hTable.put(put);
									  addCount++;
//									  Logger.getRootLogger().info("###HBASE put ROWKEY:" + row.getKey());
									  //Logger.getRootLogger().info("Insert Table ROW" + tableName);
								 }
								  catch(Exception ex)
								  {
									  Logger.getRootLogger().error("PUTERROR:*********" + ex.getMessage(),ex);
								  }
							  }
							  
							  rMap.getRow().clear();
							 
							  HashMap<String, CounterMap> counterRows = rMap.getCounterRow();
							  
							  for (Entry<String, CounterMap> countRow : counterRows.entrySet()) {
								  try
								  {
									  CounterMap pastCounterMap = countRow.getValue();
							          //counterRows.put(countRow.getKey(), new CounterMap());
		
							          Increment increment = new Increment(Bytes.toBytes(countRow.getKey()));
		
							          boolean hasColumns = false;
							          for (Entry<String, Counter> entry2 : pastCounterMap.entrySet()) {
							        	  
							        	  String key = entry2.getKey();
							        	  String cf = key.split(":",-1)[0];
							        	  String colum = key.split(":",-1)[1];
							            increment.addColumn(Bytes.toBytes(cf),
							                Bytes.toBytes(colum), entry2.getValue().value);
							            hasColumns = true;
							            
							          }
							          if (hasColumns) {
							        	  //updateLastUsed();
							        	  hTable.increment(increment);
							        	  increCount++;
							        	  //System.out.println("##########HBASE###############");
							          }
							          
								  }
								  catch(Exception ex)
								  {
									  Logger.getRootLogger().error("Increment ERROR:*********" + ex.getMessage(),ex);
								  }
							  }
							  
							  rMap.getCounterRow().clear();
							  
							  
							  if(addCount > 0 || increCount > 0)
							  {
								  //Logger.getRootLogger().info("DriverServer:*********" + _driverServer);
								  //Logger.getRootLogger().info("Server:*********" + _driverServer.split(":",-1)[0]);
								  //Logger.getRootLogger().info("Port:*********" + _driverServer.split(":",-1)[1]);
								  
								  Slave s = new Slave(_driverServer.split(":",-1)[0],
										  Integer.parseInt(_driverServer.split(":",-1)[1]));
								  s.InsertHbaseLog(tableName, addCount, increCount);
							  }
							  //$$$$Table End
						  }
					  }
				  }
				 
			  }
			  updateLastUsed();
		  }
		  
		  public void stopLoop() {
			  continueLoop = false;
		  }
	  }
}
	  
	  


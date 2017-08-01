package com.aotain.mushroom;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import net.sf.json.JSONObject;

public class Slave {

	private Logger log = Logger.getRootLogger();
	private String server;
	private int port;
			
	public Slave(String ser,int p)
	{
		server = ser;
		port = p;
	}
	
	public void InsertHbaseLog(String tablename,int addnum,int increnum)
	{
		HBaseImportLog hbase = new HBaseImportLog();
		hbase.setMsgID(1001);
		hbase.setTableName(tablename);
		hbase.setAddNum(addnum);
		hbase.setIncreNum(increnum);
		String servername = "";
		try {
			servername = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		hbase.setServerName(servername);
 	  	JSONObject json = JSONObject.fromObject(hbase);
 		    //System.out.println(jsonObject);
 		//log.debug("1001-MSG£º" + json.toString());
 		SocketClient clt = new SocketClient(server,
 				port);
 		String Result = clt.SendMsg(json.toString());
 		if(Result.equals("Done"))
 		{
 		    log.debug("1001-MSG:Complete!");
 		}
	}
}

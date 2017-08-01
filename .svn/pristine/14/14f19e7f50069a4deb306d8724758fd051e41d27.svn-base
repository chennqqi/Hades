package com.aotain.project.apollo.bolt;

import java.util.HashMap;
import java.util.Map;

import com.aotain.project.apollo.ApolloConfig;
import com.aotain.project.apollo.IPDatabase;
import com.aotain.project.apollo.PortInfo;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * 端口检测Bolt
 * @author Administrator
 *
 */
public class PortDetectBolt implements IBasicBolt{

	HashMap<Integer,PortInfo> ports = null;
			
	HashMap<Long,IPDatabase> map = null;
	
	Long[] IPs = null;
	
	//指定需要检测的IP地址
	HashMap<String,String> ipMap = null; 
			
	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple arg0, BasicOutputCollector arg1) {
		// TODO Auto-generated method stub
		String line = arg0.getString(0);  //得到一行记录
		
		String[] items = line.split("\\|",-1);
    	
    	String destip = items[2];
    	
    	if(ipMap.containsKey(destip))
    	{
    		String destport = items[5];
    	}
    	  
    	
		
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1) {
		// TODO Auto-generated method stub
		
		//加载配置信息
		ports = new HashMap<Integer, PortInfo>();
		ports.put(22,new PortInfo(22, 80, "SSH远程协议访问"));
		ports.put(21,new PortInfo(21, 80, "ftp访问"));
		ports.put(23,new PortInfo(23, 80, "Telnet访问"));
		ports.put(3389, new PortInfo(3389, 80, "Windows远程终端访问"));
		//135、139、445、593、1025
		ports.put(135, new PortInfo(135, 80, "Windows DCOM服务访问"));
		ports.put(139, new PortInfo(139, 80, "NetBIOS/SMB服务访问"));
		ports.put(445, new PortInfo(445, 80, "公共Internet文件系统访问"));
		ports.put(1025, new PortInfo(1025, 60, "木马netspy攻击风险"));
		
		
		ApolloConfig ap = new ApolloConfig("../config/dbconfig.ini");
		map = ap.IPDataBaseMap();
		IPs = ap.StartIPs();
		
		//指定需要检测的IP地址
		ipMap = ap.CheckIPs();
		
	}

}

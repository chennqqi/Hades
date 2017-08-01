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
 * �˿ڼ��Bolt
 * @author Administrator
 *
 */
public class PortDetectBolt implements IBasicBolt{

	HashMap<Integer,PortInfo> ports = null;
			
	HashMap<Long,IPDatabase> map = null;
	
	Long[] IPs = null;
	
	//ָ����Ҫ����IP��ַ
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
		String line = arg0.getString(0);  //�õ�һ�м�¼
		
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
		
		//����������Ϣ
		ports = new HashMap<Integer, PortInfo>();
		ports.put(22,new PortInfo(22, 80, "SSHԶ��Э�����"));
		ports.put(21,new PortInfo(21, 80, "ftp����"));
		ports.put(23,new PortInfo(23, 80, "Telnet����"));
		ports.put(3389, new PortInfo(3389, 80, "WindowsԶ���ն˷���"));
		//135��139��445��593��1025
		ports.put(135, new PortInfo(135, 80, "Windows DCOM�������"));
		ports.put(139, new PortInfo(139, 80, "NetBIOS/SMB�������"));
		ports.put(445, new PortInfo(445, 80, "����Internet�ļ�ϵͳ����"));
		ports.put(1025, new PortInfo(1025, 60, "ľ��netspy��������"));
		
		
		ApolloConfig ap = new ApolloConfig("../config/dbconfig.ini");
		map = ap.IPDataBaseMap();
		IPs = ap.StartIPs();
		
		//ָ����Ҫ����IP��ַ
		ipMap = ap.CheckIPs();
		
	}

}

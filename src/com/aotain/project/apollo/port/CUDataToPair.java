package com.aotain.project.apollo.port;

import java.util.HashMap;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import com.aotain.project.apollo.PortInfo;

import scala.Tuple2;
/**
 * 端口检测，将CU数据转换成 对
 * @author Administrator
 *
 */
public class CUDataToPair implements PairFunction<Tuple2<String,String>, String, String>{
	
	
	Broadcast<HashMap<String,String>> bcIPMaps = null;
	Broadcast<HashMap<Integer,PortInfo>> bcPorts = null;
	       
	
	public CUDataToPair(Broadcast<HashMap<String,String>> IPMaps,
			Broadcast<HashMap<Integer,PortInfo>> Ports )
	{
		bcIPMaps = IPMaps;
		bcPorts = Ports;
	}

	@Override
	public Tuple2<String, String> call(Tuple2<String, String> t)
			throws Exception {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		String line = t._2();
		
    	String[] items = line.split("\\|",-1);
    	
    	String destip = items[2];
    	
    	HashMap<String,String> ipmap = bcIPMaps.getValue();
    	
    	
    	
    	String sourceip = items[2];
		String profix = sourceip.split("\\.",-1)[0];
		
    	if(!profix.equals("192"))
		{
    		if(ipmap.size() > 0 && !ipmap.containsKey(destip))
        	{
        		//Tuple2<String, String> ret = new Tuple2<String, String>(line, "100");
        		return null;
        	}
		}
    	  
    	String destport = items[5];
    	 
    	HashMap<Integer,PortInfo> postMap = bcPorts.getValue();
    	
    	if(postMap.containsKey(Integer.parseInt(destport)))
    	{
    		//判断目标端口是否存在安全隐患。在这里直接评分
    		//String key = destip + "_" + sourceip + "_" + destport + "_" + value;
    		//异常描述
    		//String value = ports.get(Integer.parseInt(destport));
    		Tuple2<String, String> ret = new Tuple2<String, String>(line, "80");
    		return ret;
    	}
    	else
    	{
    		//正常
    		//String key = destip + "_" + sourceip + "_" + destport + "_" + value;
    		Tuple2<String, String> ret = new Tuple2<String, String>(line, "100");
    		return ret;
    	}
	}

}

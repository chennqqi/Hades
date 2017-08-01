package com.aotain.project.secmonitor.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/** 
* @ClassName: UVBoltFirst 
* @Description: TODO(多bolt分别统计相关pv) 
* @author 程彬
* @date 2015�?6�?1�? 上午9:40:46 
*  
*/ 
public class UVBoltFirst implements IBasicBolt {

	Map<String,Integer> countMap = new HashMap<String,Integer>();//ippv个数
//	BasicOutputCollector collector = null;
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("date_ip","count"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		String dateStr = input.getString(0);
		String ip = input.getString(1);
		
//		System.out.println("UVBolt:dateStr====" + dateStr + " ip===" + ip);
		String key = new String(dateStr + "_" + ip);
		Integer counts = countMap.get(key);
		if(counts == null) {
			counts =  0;
		}
		counts ++;
//		System.out.println("UVBolt:count===" + counts + " key====" + key );
		countMap.put(key, counts);
		collector.emit(new Values(key,counts));
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

}

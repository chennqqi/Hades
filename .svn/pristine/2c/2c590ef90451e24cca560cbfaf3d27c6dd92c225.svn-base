package com.aotain.project.secmonitor.bolt;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/** 
 * 汇�?�计�?
 * @ClassName: SumCalBolt 
 * @Description: TODO(这里用一句话描述这个类的作用) 
 * @author 程彬
 * @date 2015�?7�?13�? 下午2:52:21 
 *  
 */ 
public class SumCalBolt implements IBasicBolt {

	/** 
	 * @Fields serialVersionUID : TODO(用一句话描述这个变量表示�?�?) 
	 */ 
	private static final long serialVersionUID = 1L;
	Map<String,Integer> countMap = new HashMap<String,Integer>();

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

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
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		String urlKey = tuple.getString(0);
		Integer count = tuple.getInteger(1);
		int PV = 0;//总pv�?
		double totalPInfo = 0;
		
		countMap.put(urlKey, count);
		
		Iterator<String> iter1 = countMap.keySet().iterator();
		while(iter1.hasNext()) {
			String str = iter1.next();
			if(str != null) {
				PV += countMap.get(urlKey);
			}
			System.err.println("urlKey: "+ str +" count:" + countMap.get(str) + " PV =" + PV);
		}
		
		Iterator<String> iter2 = countMap.keySet().iterator();
		while(iter2.hasNext()) {
			String str = iter2.next();
			if(str != null) {
				double p = countMap.get(str)/(double)PV;//每个URL概率
				double pInfo = -Math.log(p)*p;//信息熵公�?
				System.err.println("p=" + p + " pInfo=" + pInfo);
				totalPInfo += pInfo;
			}
		}
		
		System.err.println("熵�?�：" + totalPInfo);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

}

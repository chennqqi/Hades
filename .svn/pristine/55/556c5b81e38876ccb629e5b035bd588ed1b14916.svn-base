package com.aotain.project.secmonitor.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 *结果汇�??
 * @ClassName: CCResultBolt 
 * @Description: TODO(这里用一句话描述这个类的作用) 
 * @author 程彬
 * @date 2015�?7�?15�? 上午11:31:07 
 *
 */
public class CCResultBolt implements IBasicBolt {

	/** 
	 * @Fields serialVersionUID : 
	 */ 
	private static final long serialVersionUID = 1L;

	Map<String,Double> dipPInfo = new HashMap<String,Double>(); 

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
		String dip = tuple.getString(0);
		double pInfo = tuple.getDouble(1);
		
		dipPInfo.put(dip, pInfo);
		System.err.println("CCResultBolt:size="+dipPInfo.size()+" --dip:" + dip + " pInfo:" + pInfo);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

}

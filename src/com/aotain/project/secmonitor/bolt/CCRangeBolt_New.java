package com.aotain.project.secmonitor.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.aotain.project.secmonitor.utils.Constants;
import com.aotain.project.secmonitor.utils.TupleHelpers;

public class CCRangeBolt_New implements IBasicBolt {

	/** 
	 * @Fields serialVersionUID : TODO(用一句话描述这个变量表示�?�?) 
	 */ 
	private static final long serialVersionUID = 1L;
	
	Map<String,Map<String,Integer>> dipMap = new HashMap<String,Map<String,Integer>>();//不同dip的分类pv
	Map<String,Double> dipPInfo = new HashMap<String,Double>();//各个目标Ip的熵�?
	Map<String,List<String>> dipUrlMap = new HashMap<String,List<String>>();//目标IP的URL集合
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("dip","pInfo"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String,Object> conf = new HashMap<String,Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, Constants.CheckCCEmitFrequency);  
		return conf;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		try {
			if(TupleHelpers.isTickTuple(tuple)) {
				emitCountingData(collector);  
			} else {
				countInLocal(tuple); 
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new FailedException("CCRangeBolt出异�?");
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	//计算�?要的�?
	private void countInLocal(Tuple tuple) {

		String urlKey = tuple.getString(0);
		String dip = tuple.getString(1);
		
		List<String> urlList = dipUrlMap.get(dip);
		if(urlList == null) {
			urlList = new ArrayList<String>();
		}
		
		urlList.add(urlKey);
		dipUrlMap.put(dip, urlList);
		
		Iterator<String> iter1 = dipUrlMap.keySet().iterator();
		//不同目标IP的分类统�?
		while(iter1.hasNext()) {
			String dipStr = iter1.next();
			if(dipStr != null) {
				List<String> urls = dipUrlMap.get(dipStr);
				Map<String,Integer> urlCount = new HashMap<String,Integer>();
				int totalCount = 0;//当前DIP下的总PV�?
				
				//统计各个url出现的次�?
				for(Iterator<String> iter=urls.iterator();iter.hasNext();) {
					String url = iter.next();
					Integer count = urlCount.get(url);
					if(count == null) {
						count = 0;
					}
					count ++;
					urlCount.put(url, count);
				}
				
				//求�?�PV�?
				Iterator<String> iter2 = urlCount.keySet().iterator();
				while(iter2.hasNext()) {
					String url = iter2.next();
					if(url != null) {
						totalCount += urlCount.get(url);
					}
				}
				
				double totalPInfo = 0.0;//熵�??
				Iterator<String> iter3 = urlCount.keySet().iterator();
				while(iter3.hasNext()) {
					String url = iter3.next();
					if(url != null) {
						double p = urlCount.get(url)/(double)totalCount;//每个url出现的概�?
						double pInfo = -Math.log(p)*p;
						totalPInfo += pInfo;
					}
				}
				
//				System.err.println("DIP:" + dipStr +"的熵值为�?" +totalPInfo);
				dipPInfo.put(dipStr, totalPInfo);
				
			}
		}
	}

	//定时发�??
	private void emitCountingData(BasicOutputCollector collector) {
		System.err.println("currenttime"+System.currentTimeMillis());
		for(Map.Entry<String, Double> entry : dipPInfo.entrySet()) {
			
			collector.emit(new Values(entry.getKey(),entry.getValue()));
		}
	}

}

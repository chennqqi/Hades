package com.aotain.project.secmonitor.bolt;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.twitter.chill.Base64;

public class CCFmtBolt_New implements IBasicBolt {

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("urlkey","dip"));
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

	int n = 0;
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// 发�?�URL
		try {
			String logString  = tuple.getString(0);
			if(logString != null && !"".equals(logString)) {
				String[] splits = logString.split("\\|",-1);
				if(splits.length>=9) {
					String urlBase64 = splits[7];
					byte[] urlBytes;
					if(urlBase64 != null && !"".equals(urlBase64)){
						urlBytes = Base64.decode(urlBase64);
						String url = new String(urlBytes);//changyan.sohu.com/api/2/topic/comments?page_size=10&page_no=1&client_id=cyrwy0ixu&style=&topi
						//						System.err.println(n + "-URL:"+url);

						String[] urlSplits = url.split("\\/",-1);
						String key = urlSplits[0];
//						System.err.println("CCFmtBolt:" + key + " " + splits[2]);
						collector.emit(new Values(key,splits[2]));
					}
				}
				n++;
				System.out.println("linenumber======" + n);
			} else {
				System.err.println("***********************END*************************");
				System.err.println("***********************END*************************");
				System.err.println("***********************END*************************");
				System.err.println("***********************END*************************");
			}
			//这里不需要手动调用ack（）方法�?
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new FailedException("CCRangeBolt消息处理失败�?");//显示抛出FailedException Storm会自动处�?
		}

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

}

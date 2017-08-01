package com.aotain.project.secmonitor.bolt;

import java.util.Date;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.aotain.project.secmonitor.utils.DateFmt;

public class LogFmtBolt implements IBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6989827522935022242L;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("date","ip"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		String logString = input.getString(0);
//		System.err.println("log====="+logString);
		if(logString != null && !"".equals(logString)) {
			String[] splits = logString.split("\\|",-1);
			if(splits.length>=9) {
				Date dStartTime = new Date(Long.parseLong(splits[8].trim())*1000L);
				String dateStr = DateFmt.getCountDate(dStartTime, DateFmt.date_short);
				collector.emit(new Values(dateStr,splits[1]));
			}
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

}

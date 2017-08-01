package com.aotain.project.apollo.bolt;

import java.util.Date;
import java.util.Map;

import com.aotain.project.secmonitor.utils.DateFmt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BaseStatBolt implements IBasicBolt{

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		//����������ֶ�����
		declarer.declare(new Fields("destip"));
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
		//��kafka spout��ȡ�� ������־������
		 /*
	     *      houseid �������
				sourceip ԴIP
				destip Ŀ��IP
				Э������
				sourceport Դ�˿�
				destport Ŀ��˿�
				domainname ����
				url URL
				Duration ʱ��
				accesstime ����ʱ��
	     */
		String logString = input.getString(0);
		if(logString != null && !"".equals(logString)) {
			String[] items = logString.split("\\|",-1);
			if(items.length>=10) {
				String destip = items[2];
				//String sourceip = items[2];
				
				Date dStartTime = new Date(Long.parseLong(items[9].trim())*1000L);
				String dateStr = DateFmt.getCountDate(dStartTime, DateFmt.date_short);
				collector.emit(new Values(destip));
			}
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

}

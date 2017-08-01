package com.aotain.project.apollo.bolt;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.aotain.common.TupleHelpers;
import com.aotain.hbase.dataimport.HBaseRecordAdd;
import com.aotain.project.apollo.SiteEvaluateMain;
import com.aotain.project.apollo.utils.ApolloProperties;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class BaseStatSumBolt implements IBasicBolt{

	private String zooserver = "";
	private String logserver = "";
	
	HashMap<String,Integer> DestIPPV = new HashMap<String,Integer>();
	
	
	public BaseStatSumBolt()
	{
		//this(Constants.WindowLengthInSeconds,Constants.EmitFrequencyInSeconds);
	}
	
	
	public BaseStatSumBolt(String zooServer,String LogServer){
		zooserver = zooServer;
		logserver = LogServer;
		//if(windowLengthInSeconds%emitFrequencyInSeconds!=0){
		//	System.err.println("窗口个数要是整数");
		//}
		//this.windowLengthInSeconds = windowLengthInSeconds;
		//this.emitFrequencyInSeconds = emitFrequencyInSeconds;
		//		cache = new SlidingWindow(windowLengthInSeconds/emitFrequencyInSeconds);
		//cache = new SlidingWindow(20);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		// 设定时间周期（秒）
		Map<String,Object> conf = new HashMap<String,Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,300);
		return conf;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		try {
			if(TupleHelpers.isTickTuple(input)) {//满足时间条件，开始统计
				emitCountingData(collector);  
			} else {//未到达时间条件，做记录累加
				countInLocal(input); 
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new FailedException("WindowCalBolt出异常");
		}	
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	
	
	private void countInLocal(Tuple tuple) {

		String dip = tuple.getString(0);

		if(DestIPPV.get(dip) != null)
		{
			int dipPV = 0;
			dipPV = DestIPPV.get(dip) + 1;
			DestIPPV.put(dip, dipPV);
		}
		else
		{
			DestIPPV.put(dip, 1);
		}
	}
	
	private void emitCountingData(BasicOutputCollector collector) {
		
		Date date = new Date();
		SimpleDateFormat sdf  = new SimpleDateFormat("yyyyMMddHHmm");
		//SimpleDateFormat sdf_h  = new SimpleDateFormat("yyyyMMddHH");
		//SimpleDateFormat sdf_d  = new SimpleDateFormat("yyyyMMdd");
		//SimpleDateFormat sdf_hour  = new SimpleDateFormat("HH");
		String dateStr = sdf.format(date);
		//String dateStr_h = sdf_h.format(date);
		//String dateStr_d = sdf_d.format(date);
		//String dateStr_hour = sdf_hour.format(date);
		
		HBaseRecordAdd hbaseInstance = HBaseRecordAdd.getInstance(
				zooserver,logserver);
		
		
		for(String dkey : DestIPPV.keySet()) {
			int avgPvalue = DestIPPV.get(dkey);
			String rowkey = String.format("%s_%s", dkey,dateStr);
			String cf = "cf";
			
			String tbName = ApolloProperties.SDS_SESSION_STAT_H;
			
			hbaseInstance.Add(tbName, rowkey, cf,  "DESTIP", dkey);
			hbaseInstance.Add(tbName, rowkey, cf,  "REPORTTIME", dateStr);
			hbaseInstance.incerment(tbName, rowkey, "cf:PV", avgPvalue);
			
			
			SimpleDateFormat dfDay = new SimpleDateFormat("yyyyMMdd");
			Date dStartTimeDay = new Date();
			String strDateDay = dfDay.format(dStartTimeDay);
			
			//IP 评估
			SiteEvaluateMain.getInstance(zooserver,logserver).EvaluateFunction(dkey, strDateDay, strDateDay + "1");
               
		}
		
	}

}

package com.aotain.project.secmonitor.bolt;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.aotain.project.secmonitor.utils.DateFmt;

/** 
 * 汇�?�pvuv统计结果
 * @ClassName: UVPVSumBolt 
 * @Description: TODO(这里用一句话描述这个类的作用) 
 * @author 程彬
 * @date 2015�?6�?1�? 上午11:21:36 
 *  
 */ 
public class UVPVSumBolt implements IBasicBolt {

	Map<String,Integer> map = new HashMap<String,Integer>();
	public String currDate = null;
	public Long beginTime = System.currentTimeMillis(); 
	public Long endTime = 0L;

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
		currDate = DateFmt.getCountDate("2015-05-26", DateFmt.date_short);
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		try {
			endTime = System.currentTimeMillis();
			Integer PV = 0;
			Integer UV = 0;
			String dateIp = null;
			Integer count = 0;

			dateIp = input.getString(0);
			count = input.getInteger(1);

			//按天统计就需要有跨天的�?�虑，如果为第二天的数据则清空map，currdate设为跨天的日�?
//			if(!dateIp.startsWith(currDate) &&
//					DateFmt.parseDate(dateIp.split("_")[0]).after(DateFmt.parseDate(currDate))) {
//				map.clear();
//				currDate = dateIp.split("_")[0];
//			}
  //			System.out.println("UVPVSumBolt:dateIP==="+dateIp+" counts==="+count );
			map.put(dateIp, count);

			System.err.println("*********mapsize====="+map.size() + "");
			//�? 秒汇总一�?
//			if(endTime - beginTime >= 3000) {
				Iterator<String> iter = map.keySet().iterator();
				while(iter.hasNext()) {
					String str = iter.next();
					if(str != null) {
						//这里要排出前�?天的因为延迟发�?�过来的数据
//						if( str.startsWith(currDate)) {
							UV ++;
							PV += map.get(str);
//						}
					}
				}
				System.err.println("PV=" + PV + ";  UV="+ UV + " currtime:" + new Date(System.currentTimeMillis()));
							beginTime = System.currentTimeMillis();
//			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

}

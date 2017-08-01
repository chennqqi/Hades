package com.aotain.project.secmonitor.spout;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 源数据Spout
 * @author cheng
 *
 */
public class SourceDataSpout implements IRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 215561769164312004L;
	
	Integer TaskId = null;
	SpoutOutputCollector collector  = null;
	Queue<String> queue = new ConcurrentLinkedQueue<String>();
	
	String topic = "";
	
	public SourceDataSpout() {
		
	}
	
	public SourceDataSpout(String topic) {
		this.topic = topic;
	}
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
//		SourceConsumer consumer = new SourceConsumer(topic);
//		consumer.start();
//		queue = consumer.getQueue();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
//		if(queue.size()>0) {
//			String str = queue.poll() ;
//			collector.emit(new Values(str)) ;
//		}
		collector.emit(new Values("440300|075500238882@163.gd|113.92.215.86|q.qlogo.cn|http://q.qlogo.cn/qqapp/1000000780/63E822F3A4505C7A64A9CD77342C3517/40|||||||1432600718|||0||0"));
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("log"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}

package com.aotain.project.secmonitor.utils;

import java.util.ArrayList;
import java.util.List;

import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.spout.SchemeAsMultiScheme;

public class KafkaConfigUtil {

	String topic = null;
	static SpoutConfig spoutConfig = null;
	public KafkaConfigUtil(String topic,String zkRoot,String id) {
		this.topic = topic;
		ZkHosts zkHosts = new ZkHosts(KafkaProperties.zkConnect);
		spoutConfig = new SpoutConfig(zkHosts,topic,zkRoot,id);
		List<String> zkServers = new ArrayList<String>() ;
		for(String host : zkHosts.brokerZkStr.split(","))
		{
			zkServers.add(host.split(":")[0]);
		}
		spoutConfig.zkServers = zkServers ;
		spoutConfig.zkPort = 2181;
		spoutConfig.forceFromStart = false; //是否从头�?始消�?
		spoutConfig.socketTimeoutMs = 60 * 1000 ;//zookeeper超时时间
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme()) ; //以字符串形式传�??
	}
	
	public SpoutConfig getConfig() {
		return spoutConfig;
	}
}

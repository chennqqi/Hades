package com.aotain.common;

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
		spoutConfig.forceFromStart = false; //
		spoutConfig.socketTimeoutMs = 60 * 1000 ;//
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme()) ; //
	}
	
	public SpoutConfig getConfig() {
		return spoutConfig;
	}
}

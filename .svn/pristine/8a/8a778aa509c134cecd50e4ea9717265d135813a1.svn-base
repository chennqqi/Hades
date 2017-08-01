package com.aotain.project.apollo.topo;

import storm.kafka.KafkaSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import com.aotain.project.apollo.bolt.PortDetectBolt;
import com.aotain.common.KafkaConfigUtil;
import com.aotain.common.KafkaProperties;

public class PortDetectTopo {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		
		//通过 Kafka 获取数据
		builder.setSpout("sourceSpout", new KafkaSpout(new KafkaConfigUtil(KafkaProperties.Log_topic,
				KafkaProperties.zkRoot,KafkaProperties.Log_PortDetect_groupId).getConfig()),10);
		
		
		builder.setBolt("portdetect", new PortDetectBolt()).shuffleGrouping("sourceSpout");
		//builder.setBolt("print", new PrintBolt()).shuffleGrouping("exclaim");

		Config conf = new Config();
		conf.setDebug(false);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(8);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(100000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}
}

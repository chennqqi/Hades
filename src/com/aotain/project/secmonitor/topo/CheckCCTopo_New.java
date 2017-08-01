package com.aotain.project.secmonitor.topo;

import storm.kafka.KafkaSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.aotain.project.secmonitor.bolt.CCFmtBolt_New;
import com.aotain.project.secmonitor.bolt.CCRangeBolt_New;
import com.aotain.project.secmonitor.bolt.CCResultBolt;
import com.aotain.project.secmonitor.utils.KafkaConfigUtil;
import com.aotain.project.secmonitor.utils.KafkaProperties;

public class CheckCCTopo_New {

	public static void main(String[] args) {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("sourceSpout", new KafkaSpout(new KafkaConfigUtil(KafkaProperties.Log_topic,KafkaProperties.zkRoot,KafkaProperties.Log_CheckCC_groupId).getConfig()),10);
		builder.setBolt("CCFmtBolt_New", new CCFmtBolt_New(),5).shuffleGrouping("sourceSpout");
		builder.setBolt("CCRangeBolt_New", new CCRangeBolt_New(),5).fieldsGrouping("CCFmtBolt_New", new Fields("dip"));
		builder.setBolt("CCResultBolt", new CCResultBolt(),1).shuffleGrouping("CCRangeBolt_New");
		
		Config conf = new Config();
//		conf.setNumWorkers(9);
//		conf.put("topology.acker.executors", 0);
		conf.setDebug(false);
		
		if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
		}else {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("CheckCCTopo_New", conf, builder.createTopology());
		}
	}

}

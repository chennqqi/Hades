package com.aotain.project.secmonitor.topo;

import storm.kafka.KafkaSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.aotain.project.secmonitor.bolt.LogFmtBolt;
import com.aotain.project.secmonitor.bolt.UVBoltFirst;
import com.aotain.project.secmonitor.bolt.UVPVSumBolt;
import com.aotain.project.secmonitor.utils.KafkaConfigUtil;
import com.aotain.project.secmonitor.utils.KafkaProperties;


public class UVPVTopo {

	public static void main(String[] args) {
		
		TopologyBuilder builder = new TopologyBuilder();
//		builder.setSpout("sourceSpout", new SourceDataSpout(KafkaProperties.Log_topic),5);
		builder.setSpout("sourceSpout", new KafkaSpout(new KafkaConfigUtil(KafkaProperties.Log_topic,KafkaProperties.zkRoot,"group1").getConfig()),10);
		builder.setBolt("FmtBolt", new LogFmtBolt(),5).shuffleGrouping("sourceSpout");
		builder.setBolt("eachPvBolt", new UVBoltFirst(),5).fieldsGrouping("FmtBolt", new Fields("ip"));
		builder.setBolt("resultBolt", new UVPVSumBolt(),1).shuffleGrouping("eachPvBolt");
		
		Config conf = new Config();
		conf.setNumWorkers(3);
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
			localCluster.submitTopology("pvuv", conf, builder.createTopology());
		}
	}

}

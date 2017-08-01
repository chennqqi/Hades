package com.aotain.project.apollo.topo;

import java.net.InetAddress;
import java.net.UnknownHostException;

import storm.kafka.KafkaSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.aotain.common.KafkaConfigUtil;
import com.aotain.common.KafkaProperties;
import com.aotain.mushroom.Master;
import com.aotain.project.apollo.bolt.BaseStatBolt;
import com.aotain.project.apollo.bolt.BaseStatSumBolt;
import com.aotain.project.apollo.utils.ApolloProperties;

public class BaseStatTopo {
	public static void main(String[] args) throws Exception {
		
		if (args.length != 1){
			   System.err.printf("Usage: <ZooServer>");
			   System.exit(1);
			}        
		
		
		String zkQuorum = args[0];
		
		String servername = "";
		try {
			servername = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		servername = servername + ":9529";
		
		Master.getInstance().StartMaster(9529);
		
		
		TopologyBuilder builder = new TopologyBuilder();
		
		//通过 Kafka 获取数据
//		builder.setSpout("sourceSpout1", new KafkaSpout(new KafkaConfigUtil(ApolloProperties.BaseStatKafka1,
//				zkQuorum,KafkaProperties.Log_Stat_groupId).getConfig()),4);
		
		builder.setSpout("sourceSpout2", new KafkaSpout(new KafkaConfigUtil(ApolloProperties.BaseStatKafka2,
				KafkaProperties.zkRoot,KafkaProperties.Log_Stat_groupId).getConfig()),4);
		
		builder.setBolt("basestat", new BaseStatBolt(),4).shuffleGrouping("sourceSpout2");
		builder.setBolt("basestatsum", new BaseStatSumBolt(zkQuorum,servername),4).fieldsGrouping("basestat", new Fields("destip"));

		Config conf = new Config();
		conf.setDebug(false);

		conf.setNumWorkers(4);
		StormSubmitter.submitTopology("basestat", conf, builder.createTopology());
		
		/*if (args != null && args.length > 0) {
			conf.setNumWorkers(4);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(100000);
			cluster.killTopology("test");
			cluster.shutdown();
		}*/
	}
}

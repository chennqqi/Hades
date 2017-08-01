package com.aotain.project.ud3clear;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;






public class Topo {

	/**
	 * @param args
	 */
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Ud3Config uc=new Ud3Config();
		uc.setConfig();
		TopologyBuilder builder = new TopologyBuilder();

		String []iniConfigs=uc.iniConfigs;
		  //String zks = "172.16.1.34:2181,172.16.1.35:2181,172.16.1.41:2181";
		  //String zks = "192.168.5.95:2181,192.168.5.96:2181,192.168.5.97:2181";
		String zks=iniConfigs[0];
		           // String topic = "httpgets";
		String topic=iniConfigs[1];
		        String zkRoot = "/storm"; // default zookeeper root configuration for storm
		
		            String id = iniConfigs[7];
		           
		            String areaid=iniConfigs[8];
		            String configpath=iniConfigs[9];
		            ZkHosts brokerHosts = new ZkHosts(zks);
		            if(!iniConfigs[6].equals("default"))
		            	brokerHosts.brokerZkPath=iniConfigs[6];
		            SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
		        
		            spoutConf.scheme= new SchemeAsMultiScheme(new StringScheme());
		 
		         //   spoutConf.forceFromStart = false;
		        
		           String zkser=iniConfigs[2];
		           String []zkservers=zkser.split(",");
		          // spoutConf.zkServers = Arrays.asList(new String[] {"192.168.5.95", "192.168.5.96", "192.168.5.97"});
		            //spoutConf.zkServers = Arrays.asList(new String[] {"172.16.1.34", "172.16.1.35", "172.16.1.41"});
		           spoutConf.zkServers=Arrays.asList(zkservers);
		            spoutConf.zkPort = 2181;
		            int kafkaspout=Integer.parseInt(iniConfigs[3]);
		            builder.setSpout("kafka-reader", new KafkaSpout(spoutConf),kafkaspout); 
		          
		         
		
		Config conf = new Config() ;
	
		
		conf.setDebug(false);
		if (args.length > 0) {
			try {
				uc.setBathconfigPath(args[1]);
				uc.setOutpath(args[2]);
				uc.setHdfsoutpath(args[3]);
				
				TestBolt t=new TestBolt();
				t.setBrowserRegexs(uc.getBrowserRegexs());
				t.setBrowserVersionRegexs(uc.getBrowserVersionRegexs());
				//t.setClassInfo(uc.getClassInfo());
				t.setAreaid(areaid);
				t.setConfigpath(configpath);
				t.setHdfsoutpath(uc.getHdfsoutpath());
				t.setKwc(uc.getKwc());
				t.setOutpath(uc.getOutpath());
				t.setOsRegexs(uc.getOsRegexs());
				t.setStuffConfig(uc.getStuffConfig());
				System.err.println("shuchu");
				System.err.println("------kaisshi"+uc.getStuffConfig().size());
				int numWorkers=Integer.parseInt(iniConfigs[4]);
				conf.setNumWorkers(numWorkers);
				conf.setNumAckers(0);
				int boltnum=Integer.parseInt(iniConfigs[5]);
				 builder.setBolt("filter", t ,boltnum).shuffleGrouping("kafka-reader") ;
				t.setOsVersionRegexs(uc.getOsVersionRegexs());
				t.setDeviceRegexs(uc.getDeviceRegexs());
				 conf.setMessageTimeoutSecs(60);
				
				
				
					try {
						StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
					} catch (AuthorizationException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
		}
	}

}

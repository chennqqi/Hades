package com.aotain.project.apollo;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import com.aotain.hbase.dataimport.HBaseRecordAdd;
import com.aotain.project.apollo.utils.ApolloProperties;


/**
 * 待检测IP主动请求行为检测
 * @author Administrator
 *
 */
public class ReverseDetect {

	 public static void main(String[] args) {
		   //System.out.println("0###OK################################");
		
		if (args.length != 1){
		   System.err.printf("Usage: <ZooServer>");
		   System.exit(1);
		}          
		
		
		
		//HTTPDCSpark spark = new HTTPDCSpark();
		int nexit = SparkStreaming(args);
		   //System.out.println("1###OK################################");
		System.exit(nexit);
	 }
	 
	 
	 //@SuppressWarnings("serial")
	 public static int SparkStreaming(final String[] args){
			
		 String zkQuorum = args[0];
		 String columnFamily = "cf";
		 String group = "rev-consumer-group";
		 String numThread = "4"; //对应kafka partition	
//		 ApolloConfig ap = new ApolloConfig("../config/dbconfig.ini");
			
		 //指定需要检测的IP地址
//		 HashMap<String,String> ipMap = ap.CheckIPs();
		 
		 
		 PortImmune pi = new PortImmune();
		 HashMap<String,HashMap<Integer,Integer>> immPortMap = pi.getPortImmune();
					
		 SparkConf conf = new SparkConf().setAppName("Reverse Detect");
		 JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.minutes(1));
			
		 final Broadcast<String> bcZooServer =
				 jssc.sparkContext().broadcast(zkQuorum);
			
		 final Broadcast<String> broadcastColumnFamily =
				 jssc.sparkContext().broadcast(columnFamily);
			
		 //检测的IP集合
//		 final Broadcast<HashMap<String,String>> bcIPMaps =
//				 jssc.sparkContext().broadcast(ipMap);
		 
		 
		 final Broadcast<HashMap<String,HashMap<Integer,Integer>>> bcImmPortMaps =
				 jssc.sparkContext().broadcast(immPortMap);
			
		 final Broadcast<String> bcSessStatHour =
				 jssc.sparkContext().broadcast(ApolloProperties.SDS_SESSION_REV_STAT);
			
		 int numThreads = Integer.parseInt(numThread);
		 Map<String, Integer> topicMap = new HashMap<String, Integer>();
		 String[] topics = ApolloProperties.BaseStatKafkaTopic.split(",");
		 for (String topic : topics) {
			 topicMap.put(topic, numThreads);
		 }
		    
		    
		 /*
		  *      houseid 机房编号
				sourceip 源IP
					destip 目标IP
					协议类型
					sourceport 源端口
					destport 目标端口
					domainname 域名
					url URL
					Duration 时长
					accesstime 访问时间
		     */
			JavaPairReceiverInputDStream<String, String> messages =
			        KafkaUtils.createStream(jssc, zkQuorum, group,
			        		topicMap);
					
			messages.persist(StorageLevel.MEMORY_AND_DISK_SER());
			
			
			JavaPairDStream<String,Integer> lines =
					messages.mapToPair(new PairFunction<Tuple2<String,String>, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(Tuple2<String, String> t)
							throws Exception {
							// TODO Auto-generated method stub
						String line = t._2();
						String[] items = line.split("\\|",-1);
						//String destip = items[2];
						String sourceip = items[1];
						int sourceport = Integer.parseInt(items[4]);
						        	
						//HashMap<String,String> ipmap = bcIPMaps.getValue();
						
						HashMap<String,HashMap<Integer,Integer>> immPortMap = bcImmPortMaps.getValue();
						        	
						//if(ipmap.size() > 0 && !ipmap.containsKey(sourceip))
						//{
//							//Tuple2<String, String> ret = new Tuple2<String, String>(line, "100");
//							return null;
//						}
						
						if(immPortMap.containsKey(sourceip))
						{
							HashMap<Integer,Integer> ports = immPortMap.get(sourceip);
							if(!ports.containsKey(sourceport))
							{
								Tuple2<String, Integer> ret = new Tuple2<String, Integer>(sourceip, 1);
								return ret;
							}
							else
							{
								return null;
							}
						}
						else
						{
							return null;
						}

						
					}
					}).filter(new Function<Tuple2<String, Integer>,Boolean>(){
					
			        	@Override
						public Boolean call(Tuple2<String, Integer> v1)
								throws Exception {
							// TODO Auto-generated method stub
							if(v1 == null)
								return false;
							else
								return true;
						}
			        });
			
			JavaPairDStream<String, Integer>  collection = lines.reduceByKey(
					new Function2<Integer, Integer, Integer>()
					{//计算每一个源->>>>>目标的IP得分
					
						@Override
						public Integer call(Integer v1, Integer v2) throws Exception {
							// TODO Auto-generated method stub
							return (v1 + v2);
						}	
					}
					
				);
			
			
			collection.foreach(new Function2<JavaPairRDD<String, Integer>, Time, Void>() {

				@Override
				public Void call(JavaPairRDD<String, Integer> v1, Time v2)
						throws Exception {
					// TODO Auto-generated method stub
					final long time = v2.copy$default$1();
					// TODO Auto-generated method stub
					v1.foreach(new VoidFunction<Tuple2<String, Integer>>(){
						@Override
						public void call(Tuple2<String, Integer> tuple) throws Exception {
							/*流量统计*/
			            	HBaseRecordAdd hbaseInstance = HBaseRecordAdd.getInstance(
			            			bcZooServer.getValue(),bcZooServer.getValue());
							
			            	String sourceip = tuple._1;
			            	int pv = tuple._2;
			            	
			            	SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmm");
							Date dStartTime = new Date(time);
							String strDate = df.format(dStartTime);
							
							String cf = broadcastColumnFamily.value();
							
							String rowkey = String.format("%s_%s", sourceip,strDate);
							
							String tbName = bcSessStatHour.value();
							
							hbaseInstance.Add(tbName, rowkey, cf,  "IP", sourceip);
							hbaseInstance.Add(tbName, rowkey, cf,  "REPORTTIME", strDate);
							hbaseInstance.incerment(tbName, rowkey, "cf:PV", pv);
						}
					});
					return null;
				}
			});
			
			jssc.start(); // Start the computation
			jssc.awaitTermination(); // Wait for the computation to terminate
			
			
		 return 0;
	 }
}

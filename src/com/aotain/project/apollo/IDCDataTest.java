package com.aotain.project.apollo;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

/**
 * IDC数据实验类，用于验证数据趋势
 * @author Administrator
 *
 */
public class IDCDataTest {

	public static void main(String[] args) {
		   //System.out.println("0###OK################################");
		
		if (args.length != 2){
		   System.err.printf("Usage: <TABLENAME1> <TABLENAME2>");
		   System.exit(1);
		}          
		
		//HTTPDCSpark spark = new HTTPDCSpark();
		int nexit = SparkStreaming(args);
		   //System.out.println("1###OK################################");
		System.exit(nexit);
	}
	
	public static int SparkStreaming(final String[] args){
		
		String tableName = args[0];
		String DestIPtableName = args[1];
		String columnFamily = "cf";
		
		//##zookeeper 配置
		String zkQuorum = "shanghai-cm-1";
		String group = "test-consumer-group";
		
		String topicss = "turk";
		String numThread = "64";
		
		//final String DestIP = args[2];// "222.73.174.167";
		 
		
		SparkConf conf = new SparkConf().setAppName("IDC DATA TEST");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(30));
		
		
		final Broadcast<String> broadcastTableName =
		        jssc.sparkContext().broadcast(tableName);
		final Broadcast<String> broadcastColumnFamily =
		        jssc.sparkContext().broadcast(columnFamily);
		
		final Broadcast<String> broadcastDestIPTableName =
		        jssc.sparkContext().broadcast(DestIPtableName);
		
		//final Broadcast<HashMap> broadcastDestPV = jssc.sparkContext().broadcast(new HashMap());
		//final Broadcast<HashMap> broadcastDestInfoValue = jssc.sparkContext().broadcast(new HashMap());
		    
		int numThreads = Integer.parseInt(numThread);
	    Map<String, Integer> topicMap = new HashMap<String, Integer>();
	    String[] topics = topicss.split(",");
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
		        KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);

		
		
		//获取IP地址  DIP SIP DPORT ----基于源IP的统计   DATA1
		JavaPairDStream<String,Integer> CounterSource =
		        messages.mapToPair(new PairFunction<Tuple2<String,String>, String, Integer>() {
		        	/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(Tuple2<String, String> t)
							throws Exception {
						// TODO Auto-generated method stub
		        		String line = t._2();
		        		
			        	String[] items = line.split("\\|",-1);
			        	  
			        	String destip = items[2];
			        	String sourceip = items[1];
			        	String destport = items[5];
			        	 
			        	String key = destip + "_" + sourceip + "_" + destport;
			        	Tuple2<String, Integer> ret = new Tuple2<String, Integer>(key, 1);
			        	return ret;
		        	}
		        }).reduceByKey(
						new Function2<Integer, Integer, Integer>() {
							@Override 
							public Integer call (Integer i1, Integer i2) throws Exception {
								return i1 + i2;
							}
						});
		
		
		CounterSource.print();
		
		//目标IP PV统计 计算得到每个目标IP的 PV数
		JavaPairDStream<String, Integer> CounterDest = CounterSource.mapToPair(
				new PairFunction<Tuple2<String,Integer>, String, Integer>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(Tuple2<String, Integer> t)
							throws Exception {
						// TODO Auto-generated method stub
						
						String DestIp = t._1.split("_",-1)[0];
						int PV = t._2;
						
						return new Tuple2<String, Integer>(DestIp, PV);
					}
				}).reduceByKey(
						new Function2<Integer, Integer, Integer>() {
							@Override 
							public Integer call (Integer i1, Integer i2) throws Exception {
								return i1 + i2;
							}
						});
		
		//CounterDest.saveAsNewAPIHadoopFiles(prefix, suffix, keyClass, valueClass, outputFormatClass, conf);
		
		//整理目标PV数 写入HBASE
		CounterDest.foreach(new Function2<JavaPairRDD<String, Integer>, Time, Void>() {

			@Override
	          public Void call(JavaPairRDD<String, Integer> values, Time v2)
	              throws Exception {

			 	final long time = v2.copy$default$1();
				
	        	  values.foreach(new VoidFunction<Tuple2<String, Integer>>() {
		              @Override
		              public void call(Tuple2<String, Integer> tuple) throws Exception {
			               String DestIP = tuple._1;
			               int PV = tuple._2;
			               /*
			               HBaseRecordAdd addDest = new HBaseRecordAdd(
		            				broadcastDestIPTableName.value(),
			                        broadcastColumnFamily.value());
			               
			               //HashMap mapDestPV = broadcastDestPV.value();
				           
			            	 
			            	String rowkey = tuple._1 + "_" + time;
			            	addDest.Add(rowkey, "DestIP", DestIP);
			            	addDest.Add(rowkey, "PV", String.valueOf(PV));
			            	//String infovalue = String.valueOf(mapDestInfoValue.get(destip));
			            	addDest.Add(rowkey, "InfoValue", "0.0");
  */
		              }
	            });

	            return null;
	          }
	        });
	          
		//异常日志记录
		CounterSource.foreach(new Function2<JavaPairRDD<String, Integer>, Time, Void>() {

	          @Override
	          public Void call(JavaPairRDD<String, Integer> values, Time time)
	              throws Exception {

	        	  final long t = time.copy$default$1();
	        	  values.foreach(new VoidFunction<Tuple2<String, Integer>>() {

	              @SuppressWarnings("unchecked")
	              @Override
	              public void call(Tuple2<String, Integer> tuple) throws Exception {
	                
	            	  /*异常日志*/
	            	/*
	            	HBaseRecordAdd addSource = new HBaseRecordAdd(
	            			 	broadcastTableName.value(),
		                        broadcastColumnFamily.value());
	            	
	            	HBaseRecordAdd addDest = new HBaseRecordAdd(
	            			broadcastDestIPTableName.value(),
	                        broadcastColumnFamily.value());
		            
	         		String destip = tuple._1.split("_",-1)[0];
	         		String sourceip = tuple._1.split("_",-1)[1];
	         		String destport = tuple._1.split("_",-1)[2];
	            	 
	         		String destRowkey = destip + "_" + t;
	         		String sTotalPV = addDest.getDataByKey(broadcastDestIPTableName.value(), destRowkey, 
	         				"PV");
	         		if(sTotalPV.trim().isEmpty())
	         			return;
	            	int nTotalPV = Integer.parseInt(sTotalPV);
	         		
	         		
	            	String rowkey = destip + "_" + t + "_" + destport + "_" + sourceip;
	            	addSource.Add(rowkey, "SourceIP", sourceip);
	            	addSource.Add(rowkey, "DestIP", destip);
	            	addSource.Add(rowkey, "DestPort", destport);
	            	//add.Add(rowkey, "Type", "DDOS");
	            	addSource.Add(rowkey, "PV", tuple._2().toString());
	            	
	            	double P = (double)tuple._2()/(double)nTotalPV; //概率
	            	double nInfoValue = -Math.log(P)*P; //信息熵
	            	addSource.Add(rowkey, "InfoValue", String.valueOf(nInfoValue));
	            	
	            	
	            	//###########Dest IP######################
	            	String sTotalInfoValue = addDest.getDataByKey(broadcastDestIPTableName.value(), destRowkey, 
	         				"InfoValue");
	            	if(sTotalInfoValue.trim().isEmpty())
	            		return;
	            	double dTotalInfoValue = Double.parseDouble(sTotalInfoValue);
	            	
	            	dTotalInfoValue = nInfoValue + dTotalInfoValue;
	            	addDest.Add(destRowkey, "InfoValue", String.valueOf(dTotalInfoValue));
	            	*/
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

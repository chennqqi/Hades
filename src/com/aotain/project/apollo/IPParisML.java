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
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.aotain.hbase.dataimport.HBaseCounterIncrementor;

import scala.Tuple2;

/**
 * IP对机器学习
 * 通过算法，将目标IP常用记录源IP登记入库
 * @author Administrator
 *
 */
public class IPParisML {

	public static void main(String[] args) {
		   //System.out.println("0###OK################################");
		
		/*if (args.length != 2){
		   System.err.printf("Usage: <TABLENAME1> <TABLENAME2>");
		   System.exit(1);
		}          */
		
		//HTTPDCSpark spark = new HTTPDCSpark();
		int nexit = SparkStreaming(args);
		   //System.out.println("1###OK################################");
		System.exit(nexit);
	}
	
	public static int SparkStreaming(final String[] args){
		String tableName = "IPPARIS";
		//String DestIPtableName = args[1];
		String columnFamily = "cf";
		
		String zkQuorum = "shanghai-cm-1";
		String group = "ipparis-consumer-group";
		
		
		String topicss = "turk1";  //Kafka Topic
		
		String numThread = "2";
		
		//final String DestIP = args[2];// "222.73.174.167";
		 
		
		SparkConf conf = new SparkConf().setAppName("IPParisML To Hbase");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		
		final Broadcast<String> broadcastTableName =
		        jssc.sparkContext().broadcast(tableName);
		final Broadcast<String> broadcastColumnFamily =
		        jssc.sparkContext().broadcast(columnFamily);
		
		
		int numThreads = Integer.parseInt(numThread);
	    Map<String, Integer> topicMap = new HashMap<String, Integer>();
	    String[] topics = topicss.split(",");
	    for (String topic : topics) {
	    	topicMap.put(topic, numThreads);
	    }
	    
	    JavaPairReceiverInputDStream<String, String> messages =
		        KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);
	   
	    
	    
	    JavaPairDStream<String,String> lines =
		        messages.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
		          
		        	@Override
					public Tuple2<String, String> call(Tuple2<String, String> t)
							throws Exception {
						// TODO Auto-generated method stub
		        		String line = t._2();
		        		
			        	String[] items = line.split("\\|",-1);
			        	  
			        	//解析出源、目标IP
			        	String destip = items[2];
			        	String sourceip = items[1];
			        	 
			        	String key = destip + "_" + sourceip;
			        	Tuple2<String, String> ret = new Tuple2<String, String>(key, line);
			        	return ret;
			        
		        	}
		        });
		
	    JavaPairDStream<String, Integer> pairs = lines.mapToPair(
				new PairFunction<Tuple2<String,String>, String, Integer>() {
				@Override
				public Tuple2<String, Integer> call(Tuple2<String, String> t)
						throws Exception {
					// TODO Auto-generated method stub
					
					return new Tuple2<String, Integer>(t._1, 1);
				}
			}); 
		
	    JavaPairDStream<String, Integer> Counter = pairs.reduceByKey(
				new Function2<Integer, Integer, Integer>() {
				@Override 
				public Integer call (Integer i1, Integer i2) throws Exception {
					return i1 + i2;
				}
			});  //1分钟内的PV数
		
	    
	    
	    Counter.foreach(new Function2<JavaPairRDD<String, Integer>, Time, Void>() {

	          @Override
	          public Void call(JavaPairRDD<String, Integer> values, Time time)
	              throws Exception {

	        	  final long stamptime = time.copy$default$1();
	        	  
	        	  
	        	  values.foreach(new VoidFunction<Tuple2<String, Integer>>() {

	              @Override
	              public void call(Tuple2<String, Integer> tuple) throws Exception {
	                
	            	  Date pre2Date = new Date(stamptime);
		        	  SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
		        	  
	            	  
	            	  HBaseCounterIncrementor incrementor =
	                    HBaseCounterIncrementor.getInstance(
	                        broadcastTableName.value(),
	                        broadcastColumnFamily.value());
	                incrementor.incerment(tuple._1(), df.format(pre2Date), tuple._2());
	                //System.out.println("Counter:" + tuple._1() + "," + tuple._2());
	            	 
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

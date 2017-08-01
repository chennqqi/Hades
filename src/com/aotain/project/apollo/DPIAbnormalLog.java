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

public class DPIAbnormalLog {

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
		
		String zkQuorum = "shanghai-cm-1";
		String group = "test-consumer-group";
		
		String topicss = "turk1";
		String numThread = "2";
		
		//final String DestIP = args[2];// "222.73.174.167";
		 
		
		SparkConf conf = new SparkConf().setAppName("DPI Abnormal Log");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(20));
		
		
		final Broadcast<String> broadcastTableName =
		        jssc.sparkContext().broadcast(tableName);
		final Broadcast<String> broadcastColumnFamily =
		        jssc.sparkContext().broadcast(columnFamily);
		
		final Broadcast<String> broadcastDestIPTableName =
		        jssc.sparkContext().broadcast(DestIPtableName);
		
		final Broadcast<HashMap> map = jssc.sparkContext().broadcast(new HashMap());
		    
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

		
		
		//获取IP地址  DIP SIP DPORT
		JavaPairDStream<String,String> lines =
		        messages.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
		          
		        	@Override
					public Tuple2<String, String> call(Tuple2<String, String> t)
							throws Exception {
						// TODO Auto-generated method stub
		        		String line = t._2();
		        		
			        	String[] items = line.split("\\|",-1);
			        	  
			        	String destip = items[2];
			        	String sourceip = items[1];
			        	String destport = items[5];
			        	 
			        	
			        	//if(destip.equals(DestIP))
			        	//{
			        		String key = destip + "_" + sourceip + "_" + destport;
			        		Tuple2<String, String> ret = new Tuple2<String, String>(key, line);
			        		return ret;
			        	//}
			        	//else
			        	//{
			        	//	  return null;
			        	//}
		        	}
		        }).filter(new Function<Tuple2<String, String>,Boolean>(){

					public Boolean call(Tuple2<String, String> v1) throws Exception {
						// TODO Auto-generated method stub
						if(v1 == null)
							return false;
						else
							return true;
					}
		        }
		        );
		
		
		
		
		//获取IP地址 DIP 统计窗口时间内的PV总数
		JavaPairDStream<String,String> linesAll =
				        messages.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
				          
				        	@Override
							public Tuple2<String, String> call(Tuple2<String, String> t)
									throws Exception {
								// TODO Auto-generated method stub
				        		String line = t._2();
				        		
					        	String[] items = line.split("\\|",-1);
					        	  
					        	String destip = items[2];
					        	
					        	//if(destip.equals(DestIP))
					        	//{
					        		String key = destip;
					        		Tuple2<String, String> ret = new Tuple2<String, String>(key, line);
					        		return ret;
					        	//}
					        	//else
					        	//{
					        	//	  return null;
					        	//}
				        	}
				        }).filter(new Function<Tuple2<String, String>,Boolean>(){

							public Boolean call(Tuple2<String, String> v1) throws Exception {
								// TODO Auto-generated method stub
								if(v1 == null)
									return false;
								else
									return true;
							}
				        }
				        );
	
		JavaPairDStream<String, Integer> pairs = lines.mapToPair(
				new PairFunction<Tuple2<String,String>, String, Integer>() {
				@Override
				public Tuple2<String, Integer> call(Tuple2<String, String> t)
						throws Exception {
					// TODO Auto-generated method stub
					
					return new Tuple2<String, Integer>(t._1, 1);
				}
			}); 
		
		JavaPairDStream<String, Integer> pairsAll = linesAll.mapToPair(
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
		
		
		
		JavaPairDStream<String, Integer> CounterAll = pairsAll.reduceByKey(
				new Function2<Integer, Integer, Integer>() {
				@Override 
				public Integer call (Integer i1, Integer i2) throws Exception {
					return i1 + i2;
				}
			});  //1分钟内的PV数
		
		
		//计算总PV数存入HBASE
		CounterAll.foreach(new Function2<JavaPairRDD<String, Integer>, Time, Void>() {
			@Override
			public Void call(JavaPairRDD<String, Integer> v1, Time v2)
					throws Exception {
				final long time = v2.copy$default$1();
				// TODO Auto-generated method stub
				v1.foreach(new VoidFunction<Tuple2<String, Integer>>() {
		              @Override
		              public void call(Tuple2<String, Integer> tuple) throws Exception {
		                
		            	  /*异常日志*/
		            	
		            	/*
		            	  HBaseRecordAdd addDestIP = new HBaseRecordAdd(
		            				broadcastDestIPTableName.value(),
			                        broadcastColumnFamily.value());
		         		String destip = tuple._1;
		            	 
		            	String rowkey = tuple._1 + "_" + time;
		            	addDestIP.Add(rowkey, "DestIP", destip);
		            	addDestIP.Add(rowkey, "PV", tuple._2().toString());
		            	addDestIP.Add(rowkey, "InfoValue", String.valueOf(0));*/
		              }
		            });

		            return null;
			}
		});
		
		Counter.print();
		
		
		
		
		
		//异常日志记录
		Counter.foreach(new Function2<JavaPairRDD<String, Integer>, Time, Void>() {

	          @Override
	          public Void call(JavaPairRDD<String, Integer> values, Time time)
	              throws Exception {

	        	  final long t = time.copy$default$1();
	        	  values.foreach(new VoidFunction<Tuple2<String, Integer>>() {

	              @Override
	              public void call(Tuple2<String, Integer> tuple) throws Exception {
	                
	            	  /*异常日志*/
	            	  /*
	            	HBaseRecordAdd addBuff = HBaseRecordAdd.getInstance(
	            			 	broadcastTableName.value(),
		                        broadcastColumnFamily.value());
	            	 
	            	HBaseRecordAdd addDestIP = new HBaseRecordAdd(
	            				broadcastDestIPTableName.value(),
		                        broadcastColumnFamily.value());
	            	 
	         		String destip = tuple._1.split("_",-1)[0];
	         		String sourceip = tuple._1.split("_",-1)[1];
	         		String destport = tuple._1.split("_",-1)[2];
	            	 
	         		String pvKey = destip + "_" + t;
	         		String TotalPV = addDestIP.getDataByKey(broadcastDestIPTableName.value(), 
	         				pvKey, "PV");
	         		
	         		if(TotalPV.isEmpty())
	         			return;
	            	
	            	int nTotalPV = Integer.parseInt(TotalPV);
	         		
	         		
	            	String rowkey = destip + "_" + t + "_" + destport + "_" + sourceip;
	            	addBuff.Add(rowkey, "SourceIP", sourceip);
	            	addBuff.Add(rowkey, "DestIP", destip);
	            	addBuff.Add(rowkey, "DestPort", destport);
	            	//add.Add(rowkey, "Type", "DDOS");
	            	addBuff.Add(rowkey, "PV", tuple._2().toString());
	            	
	            	double P = (double)tuple._2()/(double)nTotalPV; //概率
	            	double nInfoValue = -Math.log(P)*P; //信息熵
	            	
	            	addBuff.Add(rowkey, "InfoValue", String.valueOf(nInfoValue));
	            	
	            	
	            	//###########Dest IP######################
	            	
	            	String TotalInfoValue = addDestIP.getDataByKey(broadcastDestIPTableName.value(), 
	         				pvKey, "InfoValue");
	            	double nTotalInfoValue = 0;
	            	if(TotalInfoValue.isEmpty())
	            	{
	            		addDestIP.Add(pvKey, "InfoValue", String.valueOf(nTotalInfoValue));
	            		return;
	            	}
	            	
	            	nTotalInfoValue = nInfoValue + Double.parseDouble(TotalInfoValue);
	            	addDestIP.Add(pvKey, "InfoValue", String.valueOf(nTotalInfoValue));
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

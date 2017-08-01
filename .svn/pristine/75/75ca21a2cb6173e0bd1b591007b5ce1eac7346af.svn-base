package com.aotain.project.apollo;

import java.net.InetAddress;
import java.net.UnknownHostException;
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

import com.aotain.common.CommonFunction;
import com.aotain.hbase.dataimport.HBaseRecordAdd;
import com.aotain.mushroom.Master;
import com.aotain.project.apollo.utils.ApolloProperties;

/**
 * 基准统计，包括网站IP评估
 * @author Administrator
 *
 */
public class BaseStat {

	public static void main(String[] args) {
		   //System.out.println("0###OK################################");
		
		if (args.length != 3){
		   System.err.printf("Usage: <ZooServer>");
		   System.exit(1);
		}          
		
		//HTTPDCSpark spark = new HTTPDCSpark();
		int nexit = SparkStreaming(args);
		   //System.out.println("1###OK################################");
		System.exit(nexit);
	}
	

	public static int SparkStreaming(final String[] args){
		
		String zkQuorum = args[0];
		String kafkaPartition = args[1]; //kafka分区数
		String ImportThread = args[2]; //入库线程数
		
		String columnFamily = "cf";
		
		String group = "base-consumer-group";
		
		String numThread = kafkaPartition; //对应kafka partition
		
		ApolloConfig ap = new ApolloConfig("../config/config.ini");
		
		String servername = "";
		try {
			servername = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		servername = servername + ":9527";
		
		Master.getInstance().StartMaster(9527);
		
		
		//指定需要检测的IP地址
		HashMap<String,String> ipMap = ap.CheckIPs();
				
		SparkConf conf = new SparkConf().setAppName("Base Stat.");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.minutes(5));
		
		final Broadcast<String> bcZooServer =
		        jssc.sparkContext().broadcast(zkQuorum);
		
		final Broadcast<String> bcDriverServer =
		        jssc.sparkContext().broadcast(servername);
		
		final Broadcast<String> broadcastColumnFamily =
		        jssc.sparkContext().broadcast(columnFamily);
		
		//检测的IP集合
		final Broadcast<HashMap<String,String>> bcIPMaps =
				jssc.sparkContext().broadcast(ipMap);
		
		final Broadcast<String> bcSessStatHour =
				jssc.sparkContext().broadcast(ApolloProperties.SDS_SESSION_STAT_H);
		
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
		
		
		//
		JavaPairDStream<String,Integer> lines =
				messages.mapToPair(new PairFunction<Tuple2<String,String>, String, Integer>() {
				@Override
				public Tuple2<String, Integer> call(Tuple2<String, String> t)
						throws Exception {
						// TODO Auto-generated method stub
					String line = t._2();
					String[] items = line.split("\\|",-1);
					String destip = items[2];
					String sourceip = items[2];
					String profix = sourceip.split("\\.",-1)[0];
					if(profix.equals("192"))
					{
						Tuple2<String, Integer> ret = new Tuple2<String, Integer>(destip, 1);
						return ret;
					}
					
					HashMap<String,String> ipmap = bcIPMaps.getValue();
					if(ipmap.size() > 0 && !ipmap.containsKey(destip))
					{
						return null;
					}

					Tuple2<String, Integer> ret = new Tuple2<String, Integer>(destip, 1);
					return ret;
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
		
	
		
		collection.repartition(Integer.parseInt(ImportThread)).foreach(new Function2<JavaPairRDD<String, Integer>, Time, Void>() {

			@Override
			public Void call(JavaPairRDD<String, Integer> v1, Time v2)
					throws Exception {
				// TODO Auto-generated method stub
				final long time = v2.copy$default$1();
				// TODO Auto-generated method stub
				v1.foreach(new VoidFunction<Tuple2<String, Integer>>(){
					/**
					 * 
					 */
					private static final long serialVersionUID = -1L;

					@Override
					public void call(Tuple2<String, Integer> tuple) throws Exception {
						/*流量统计*/
		            	HBaseRecordAdd hbaseInstance = HBaseRecordAdd.getInstance(
		            			bcZooServer.getValue(),bcDriverServer.getValue());
						
		            	String destip = tuple._1;
		            	int pv = tuple._2;
		            	
		            	SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmm");
						Date dStartTime = new Date(time);
						String strDate = df.format(dStartTime);
						
						String cf = broadcastColumnFamily.value();
						
						String rowkey = String.format("%s_%s", destip,strDate);
						
						String tbName = bcSessStatHour.value();
						
						hbaseInstance.Add(tbName, rowkey, cf,  "DESTIP", destip);
						hbaseInstance.Add(tbName, rowkey, cf,  "REPORTTIME", strDate);
						hbaseInstance.incerment(tbName, rowkey, "cf:PV", pv);
						
						
						SimpleDateFormat dfDay = new SimpleDateFormat("yyyyMMdd");
						Date dStartTimeDay = new Date(time);
						String strDateDay = dfDay.format(dStartTimeDay);
						
						//IP 评估
						SiteEvaluateMain.getInstance(bcZooServer.getValue(),bcDriverServer.getValue()).EvaluateFunction(destip, strDateDay, strDateDay + "1");
                           
					        
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

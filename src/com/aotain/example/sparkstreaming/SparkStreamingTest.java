package com.aotain.example.sparkstreaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.aotain.hbase.dataimport.HBaseCounterIncrementor;

import scala.Tuple2;

/**
 * Spark �������������룬ʵ��ͨ������Э�鴫���ַ�������spark����д��hbase�Ĺ���
 * @author Administrator
 *
 */
public class SparkStreamingTest {
	public static void main(String[] args) {
		   //System.out.println("0###OK################################");
		
		if (args.length != 2){
      System.err.printf("Usage: %s <IP><Port>");
      System.exit(1);
		}          
		
		//HTTPDCSpark spark = new HTTPDCSpark();
		int nexit = TestFunc(args);
		   //System.out.println("1###OK################################");
	}
	
	public static int TestFunc(final String[] args){
		
		String IP = args[0];
		String port = args[1];
		
		String tableName = "test";// args[3];
		String columnFamily = "f";// args[4];
			    
		
		 String zkQuorum = "sz-cn-1";
		 String group = "test-consumer-group";
		 String topicss = "turk1";
		 String numThread = "2";
		    
			    
		//�������Ǵ���һ��JavaStreamingContext���� ���Ǵ������Ĺ��ܵ������. 
		//���Ǵ�����һ�����ص�StreamingContext�� ʹ�������߳�, ��������Ϊ1��.
		SparkConf conf = new SparkConf().setAppName("NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
		
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
		    
		//ʹ�����context, ���ǿ��Դ���һ��DStream�� ��������TCPԴ�������ݡ���Ҫָ���������Ͷ˿�(�� localhost �� 9999).
		//JavaReceiverInputDStream<String> lines = jssc.socketTextStream(IP, Integer.parseInt(port));
		
		JavaPairReceiverInputDStream<String, String> messages =
		        KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);

		JavaDStream<String> lines =
		        messages.map(new Function<Tuple2<String, String>, String>() {
		          @Override
		          public String call(Tuple2<String, String> tuple2) {
		            return tuple2._2();
		          }
		        });
		    
		
		//��һ�д�������ݷ��������ܵ���������. DStream��ÿ����¼��һ���ı�. ������, 
		//������ʹ�ÿո�ָ�ÿһ�У������Ϳ��Եõ��ı��еĵ��ʡ�
		JavaDStream<String> words = lines.flatMap(
			new FlatMapFunction<String, String>() {
				@Override 
				public Iterable<String> call (String x) {
					return Arrays.asList(x.split(" "));
				}
			});
		
		//flatMap ��һ��һ�Զ��DStream������ ����ԴDStream�е�ÿһ��Record�������Record�� 
		//��Щ�²�����Record�����һ���µ�DStream�� �����ǵ������У� ÿһ���ı����ֳ��˶�����ʣ� 
		//����õ�������DStream. ��һ���� ������ͳ�����µ��ʵ�
		JavaPairDStream<String, Integer> pairs = words.mapToPair(
				new PairFunction<String, String, Integer>() {
				@Override 
				public Tuple2<String, Integer> call(String s) throws Exception {
					return new Tuple2<String, Integer>(s, 1 );
				}
			});

		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
				new Function2<Integer, Integer, Integer>() {
				@Override 
				public Integer call (Integer i1, Integer i2) throws Exception {
					return i1 + i2;
				}
			});

		wordCounts.print();
		//wordCounts.foreach(foreachFunc);
		
 		
		wordCounts.foreach(new Function2<JavaPairRDD<String, Integer>, Time, Void>() {

	          @Override
	          public Void call(JavaPairRDD<String, Integer> values, Time time)
	              throws Exception {

	            values.foreach(new VoidFunction<Tuple2<String, Integer>>() {

	              @Override
	              public void call(Tuple2<String, Integer> tuple) throws Exception {
	                
	            	  
	            	  HBaseCounterIncrementor incrementor =
	                    HBaseCounterIncrementor.getInstance(
	                        broadcastTableName.value(),
	                        broadcastColumnFamily.value());
	                incrementor.incerment("Counter", tuple._1(), tuple._2());
	                System.out.println("Counter:" + tuple._1() + "," + tuple._2());
	            	 
	              }
	            });

	            return null;
	          }
	        });
	
				
		//����DStream ��mapped (one-to-one transformation) ��*(word, 1)��*��DStream ,
		//Ȼ��reduced �õ�ÿһ�����ʵ�Ƶ��. ��� wordCounts.print()���ӡ��ÿһ�������һЩ���ʵ�ͳ��ֵ�� 
		//ע�⵱��Щ��ִ��ʱ��Spark Streaming����������Щ���㣬 ����û�����ϱ�ִ�С� �����еļ�������������ǿ��Ե�������Ĵ�����������
		jssc.start(); // Start the computation
		jssc.awaitTermination(); // Wait for the computation to terminate
				
		return 0;
	}
}

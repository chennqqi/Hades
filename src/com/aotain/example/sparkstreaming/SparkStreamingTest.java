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
 * Spark 流计算样例代码，实现通过网络协议传输字符串，到spark处理，写入hbase的过程
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
		    
			    
		//首先我们创建一个JavaStreamingContext对象， 它是处理流的功能的主入口. 
		//我们创建了一个本地的StreamingContext， 使用两个线程, 批处理间隔为1秒.
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
		    
		//使用这个context, 我们可以创建一个DStream， 代表来自TCP源的流数据。需要指定主机名和端口(如 localhost 和 9999).
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
		    
		
		//这一行代表从数据服务器接受到的数据流. DStream中每条记录是一行文本. 接下来, 
		//我们想使用空格分隔每一行，这样就可以得到文本中的单词。
		JavaDStream<String> words = lines.flatMap(
			new FlatMapFunction<String, String>() {
				@Override 
				public Iterable<String> call (String x) {
					return Arrays.asList(x.split(" "));
				}
			});
		
		//flatMap 是一个一对多的DStream操作， 它从源DStream中的每一个Record产生多个Record， 
		//这些新产生的Record组成了一个新的DStream。 在我们的例子中， 每一行文本被分成了多个单词， 
		//结果得到单词流DStream. 下一步， 我们想统计以下单词的
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
	
				
		//单词DStream 被mapped (one-to-one transformation) 成*(word, 1)对*的DStream ,
		//然后reduced 得到每一批单词的频度. 最后， wordCounts.print()会打印出每一秒产生的一些单词的统计值。 
		//注意当这些行执行时，Spark Streaming仅仅设置这些计算， 它并没有马上被执行。 当所有的计算设置完后，我们可以调用下面的代码启动处理
		jssc.start(); // Start the computation
		jssc.awaitTermination(); // Wait for the computation to terminate
				
		return 0;
	}
}

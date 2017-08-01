package com.aotain.project.apollo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import com.aotain.hbase.dataimport.HBaseRecordAdd;

import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * 网站评估计算，每小时运行一次
 * 通过从上小时已入库HBASE中的数据，根据算法计算每个网站当前安全评分
 * @author Administrator
 *
 */
public class EvaluateCaluSite implements Serializable {

	  public static Logger log = Logger.getRootLogger();
	    
	    
	/**
     * spark如果计算没写在main里面,实现的类必须继承Serializable接口，<br>
     * </>否则会报 Task not serializable: java.io.NotSerializableException 异常
     */
    public static void main(String[] args) throws InterruptedException {

        new EvaluateCaluSite().start(args);

        System.exit(0);
    }
    
    
    public void start(String[] args) {
    	
    	String zkQuorum = args[0];
    	String starttime = args[1];
    	String endtime = args[2];
    	
    	SparkConf sparkConf = new SparkConf().setAppName("SDS Evaluate Calu");
	    Configuration config = new Configuration();
	    config.addResource("/etc/hadoop/conf");
	    System.out.println("#%%%HDFS:" + config.get("fs.defaultFS"));
	    JavaSparkContext sc = new JavaSparkContext(sparkConf);
	    
	    String servername = "";
		try {
			servername = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	    
//        JavaSparkContext sc = new JavaSparkContext("spark://nowledgedata-n3:7077", "hbaseTest",
//                "/home/hadoop/software/spark-0.8.1",
//                new String[]{"target/ndspark.jar", "target\\dependency\\hbase-0.94.6.jar"});

        //使用HBaseConfiguration.create()生成Configuration
        // 必须在项目classpath下放上hadoop以及hbase的配置文件。
	    
	    final Broadcast<String> bcZooServer = sc.broadcast(zkQuorum);
	    final Broadcast<String> bcDriverServer =
	    		sc.broadcast(servername);
	    
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.5.95");  //千万别忘记配置
        conf.set("hbase.zookeeper.property.clientPort","2181");
        
        conf = HBaseConfiguration.create(conf);
        
        //设置查询条件，这里值返回用户的等级
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("101.227.160.27_2015092400"));
        scan.setStopRow(Bytes.toBytes("101.227.160.27_2015092500"));
        scan.addFamily(Bytes.toBytes("cf"));
        scan.addColumn(Bytes.toBytes("PORT"), Bytes.toBytes("LOW"));
        scan.addColumn(Bytes.toBytes("SESSION"), Bytes.toBytes("MIDDLE"));

            //需要读取的hbase表名
            String tableName = "SDS_ABN_SESS_STAT_H";
            conf.set(TableInputFormat.INPUT_TABLE, tableName);
            
            ClientProtos.Scan proto = null;
			try {
				proto = ProtobufUtil.toScan(scan);
				String ScanToString = Base64.encodeBytes(proto.toByteArray());
	            conf.set(TableInputFormat.SCAN, ScanToString);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				log.error(e);
			}
            
            
            log.info("########START RDD##########");
          

            //获得hbase查询结果Result
            JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = sc.newAPIHadoopRDD(conf,
                    TableInputFormat.class, ImmutableBytesWritable.class,
                    Result.class);

            log.info("########Load DATA ##########");
            
            //从result中取出用户的等级，并且每一个算一次
            JavaPairRDD<String, Double> levels = hBaseRDD.mapToPair(
            		new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, Double>() {
                        @Override
                        public Tuple2<String, Double> call(
                                Tuple2<ImmutableBytesWritable, Result> tuple)
                                throws Exception {
                        	
                        	int nPortPV = 0;
                        	int nSessionPV = 0;
                        	
                            byte[] destIP = tuple._2().getValue(
                                    Bytes.toBytes("cf"), Bytes.toBytes("IP"));
                            
                            byte[] portPV = tuple._2().getValue(
                                    Bytes.toBytes("PORT"), Bytes.toBytes("LOW"));
                            if(portPV != null)
                            	nPortPV = Bytes.toInt(portPV);
                            
                            byte[] sessionPV = tuple._2().getValue(
                                    Bytes.toBytes("SESSION"), Bytes.toBytes("MIDDLE"));
                            if(sessionPV != null)
                            	nSessionPV = Bytes.toInt(sessionPV);
                            
                            double score = (nPortPV>0?70:100)*0.3 + (nSessionPV>0?60:100)*0.7;
                            
                            if (destIP != null) {
                                return new Tuple2<String, Double>(Bytes.toString(destIP), score);
                            }
                            return null;
                        }
                    });

            
            log.info("########DATA CALU ##########");
            
            //数据累加
            JavaPairRDD<String, Double> evaluateCollection = levels.reduceByKey(new Function2<Double, Double, Double>() {
                public Double call(Double i1, Double i2) {
                    return i1 + i2;
                }

            });
            
            log.info("########DATA CALU END ##########");
            
            final Map<String, Object> counts =  levels.countByKey();
            
            log.info("########Map:" + counts.size());
            
            for(String key : counts.keySet())
            {
            	String p = String.format("########%s,%s", key,counts.get(key));
            	//log.info(p);
            }
            
            final Map<String, Double> eva = evaluateCollection.collectAsMap();
            for(String key : eva.keySet())
            {
            	String p = String.format("########%s,%s", key,eva.get(key));
            	//log.info(p);
            	
            	
            	HBaseRecordAdd hbaseInstance = HBaseRecordAdd.getInstance(
            			bcZooServer.getValue(),bcDriverServer.getValue());
				
            	String destip = key;
            	Double pv = eva.get(key);
            	
            	long count = Long.parseLong(counts.get(destip).toString());
            	
                String score = String.valueOf(pv/count);
				//String cf = broadcastColumnFamily.value();
				
				String rowkey = String.format("%s_%s", destip,"20150924");
				
				String tbName = "SDS_EVALUATE_SITE";
				
				hbaseInstance.Add(tbName, rowkey, "cf",  "IP", destip);
				//hbaseInstance.Add(tbName, rowkey, "cf",  "REPORTTIME", strDate);
				hbaseInstance.Add(tbName, rowkey, "cf", "EVALUATE", score);
            }
            
            
           
            
            HBaseRecordAdd hbaseInstance = HBaseRecordAdd.getInstance(
        			bcZooServer.getValue(),bcDriverServer.getValue());

            hbaseInstance.ImmdiateFlashData();
            
        sc.stop();
    }
}

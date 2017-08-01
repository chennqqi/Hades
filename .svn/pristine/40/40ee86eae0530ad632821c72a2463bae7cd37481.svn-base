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

import com.aotain.hbase.dataimport.HBaseRecordAdd;
import com.aotain.mushroom.Master;
import com.aotain.project.apollo.port.AbNormalStream;
import com.aotain.project.apollo.port.CUDataToPair;
import com.aotain.project.apollo.port.StreamInit;
import com.aotain.project.apollo.utils.ApolloProperties;

import scala.Tuple2;
   
/**
 * 端口检测，发现异常的服务端口
 * @author Administrator
 *
 */
public class PortDetect {

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
	
	
	public static int SparkStreaming(final String[] args){
	
		String zkQuorum = args[0];
		
		String columnFamily = "cf";
		
		String group = "port-consumer-group";
		
		String numThread = "20";
		
		HashMap<Integer,PortInfo> ports = new HashMap<Integer, PortInfo>();
		ports.put(22,new PortInfo(22, 80, "SSH远程协议访问"));
		ports.put(21,new PortInfo(21, 80, "ftp访问"));
		ports.put(23,new PortInfo(23, 80, "Telnet访问"));
		ports.put(3389, new PortInfo(3389, 80, "Windows远程终端访问"));
		//135、139、445、593、1025
		ports.put(135, new PortInfo(135, 80, "Windows DCOM服务访问"));
		ports.put(139, new PortInfo(139, 80, "NetBIOS/SMB服务访问"));
		ports.put(445, new PortInfo(445, 80, "公共Internet文件系统访问"));
		ports.put(1025, new PortInfo(1025, 60, "木马netspy攻击风险"));
		
		
		//final String DestIP = args[2];// "222.73.174.167";
		 
		String servername = "";
		try {
			servername = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		servername = servername + ":9528";
		//蘑菇仓库
		Master.getInstance().StartMaster(9528);
		
		//从oracle获取配置数据
		ApolloConfig ap = new ApolloConfig("../config/config.ini");
		HashMap<Long,IPDatabase> map = ap.IPDataBaseMap();
		Long[] IPs = ap.StartIPs();
		//Long ip = ApolloConfig.getStartIP(ap.StartIPs(), "1.0.3.255");
		 
		//指定需要检测的IP地址
		HashMap<String,String> ipMap = ap.CheckIPs();
		
		//获取到服务器配置信息
		HashMap<String,ServerInfo> mapServers = ap.ServerInfos();
		
		SparkConf conf = new SparkConf().setAppName("Port Detect");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(60));
		
		//端口检测配置
		final Broadcast<HashMap<Integer,PortInfo>> bcPorts =
		        jssc.sparkContext().broadcast(ports);
		
		final Broadcast<String> bcZooServer =
		        jssc.sparkContext().broadcast(zkQuorum);
		
		final Broadcast<String> bcDriverServer =
		        jssc.sparkContext().broadcast(servername);
		
		//检测的IP集合
		final Broadcast<HashMap<String,String>> bcIPMaps =
		        jssc.sparkContext().broadcast(ipMap);
		
		//异常日志存储表
		final Broadcast<String> bcAbnormalTbName =
		        jssc.sparkContext().broadcast(ApolloProperties.SDS_ABNORMAL_LOG);
		
//		//正常日志存储表
//		final Broadcast<String> bcNormalTbName =
//		        jssc.sparkContext().broadcast(ApolloProperties.SDS_IDC_LOG);
//		
//		//异常评估表名
//		final Broadcast<String> bcEvaluateTbName =
//		        jssc.sparkContext().broadcast(ApolloProperties.SDS_EVALUATE_SITE);
		
		//异常来源分布表
		final Broadcast<String> bcAreaTbName =
				jssc.sparkContext().broadcast(ApolloProperties.SDS_ABNORMAL_AREA);
		
		//异常来源分布统计-天
//		final Broadcast<String> bcAreaTbNameDay =
//				jssc.sparkContext().broadcast(ApolloProperties.SDS_ABNML_SESS_AREA_STAT_D);
		
		//异常流量小时统计
		final Broadcast<String> bcAbnSessStatHour =
				jssc.sparkContext().broadcast(ApolloProperties.SDS_ABN_SESS_STAT_H);
		
		//异常流量天统计
		final Broadcast<String> bcAbnSessStatDay =
				jssc.sparkContext().broadcast(ApolloProperties.SDS_ABN_SESS_STAT_D);
		
		//异常流量来源IP统计 小时
		final Broadcast<String> bcAbnSessSourHour =
				jssc.sparkContext().broadcast(ApolloProperties.SDS_ABN_SESS_SOUR_STAT_H);
		
		//异常流量来源IP统计 天
		final Broadcast<String> bcAbnSessSourDay =
				jssc.sparkContext().broadcast(ApolloProperties.SDS_ABN_SESS_SOUR_STAT_D);	
		
		final Broadcast<String> broadcastColumnFamily =
		        jssc.sparkContext().broadcast(columnFamily);
		
		final Broadcast<HashMap<Long,IPDatabase>> bcIPMap =
		        jssc.sparkContext().broadcast(map);
		
		final Broadcast<HashMap<String,ServerInfo>> bcServerMap =
		        jssc.sparkContext().broadcast(mapServers);
		
		final Broadcast<Long[]> bcIPArray =
		        jssc.sparkContext().broadcast(IPs);
		
		int numThreads = Integer.parseInt(numThread);
	    Map<String, Integer> topicMap = new HashMap<String, Integer>();
	    String[] topics = ApolloProperties.PortDetectKafkaTopic.split(",");
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
		
		//获取IP地址  DIP SIP DPORT
		JavaPairDStream<String,String> lines =
		        messages.mapToPair(new CUDataToPair(bcIPMaps,bcPorts){}).filter(
		        		new Function<Tuple2<String, String>,Boolean>(){
					@Override
					public Boolean call(Tuple2<String, String> v1)
							throws Exception {
						// TODO Auto-generated method stub
						if(v1 == null)
							return false;
						else
							return true;
					}
		        });
		 
		//异常流量结果，源-->>目标 
		JavaPairDStream<String,Integer> AbNomallines = lines.filter(new Function<Tuple2<String, String>,Boolean>(){


			@Override
				public Boolean call(Tuple2<String, String> v1) throws Exception {
					// TODO Auto-generated method stub
					if(v1._2.equals("100"))
						return false;
					else
						return true;
					}
		    	
		    }).mapToPair(new PairFunction<Tuple2<String, String>,String, Integer>() {
				
				//计算连接的次数
				@Override
				public Tuple2<String, Integer> call(Tuple2<String, String> t)
						throws Exception {
					// TODO Auto-generated method stub
					String line = t._1;
					String score = t._2;
					String[] items = line.split("\\|",-1);
					String destip = items[2];
		        	String sourceip = items[1];
		        	String destport = items[5];
		        	//String domainname = items[6];
		        	//String desc = items[items.length-1];
		        	
		        	String key = destip + "|" + destport + "|" + sourceip 
		        			+ "|" + score;
		        	
		        	Tuple2<String, Integer> tuple = new Tuple2<String, Integer>(key, 1);
					return tuple;
				}
			}).reduceByKey(
					new Function2<Integer, Integer, Integer>()
					{//计算每一个源->>>>>目标的IP得分
					

						@Override
						public Integer call(Integer v1, Integer v2) throws Exception {
							// TODO Auto-generated method stub
							return (v1 + v2);
						}
						
					}
					
				);
		
		
		
		//正常的流量连接，源-->>目标 
		JavaPairDStream<String,Integer> Nomallines = lines.filter(new Function<Tuple2<String, String>,Boolean>(){
		


						@Override
						public Boolean call(Tuple2<String, String> v1) throws Exception {
							// TODO Auto-generated method stub
							if(v1._2.equals("100"))
								return true;
							else
								return false;
							}
				    	
				    }).mapToPair(new PairFunction<Tuple2<String, String>,String, Integer>() {

						//计算归属地的连接次数
						@Override
						public Tuple2<String, Integer> call(Tuple2<String, String> t)
								throws Exception {
							// TODO Auto-generated method stub
							String line = t._1;
							String[] items = line.split("\\|",-1);
							String destip = items[2];
				        	String sourceip = items[1];
				        	String destport = items[5];
				        	String domainname = items[6];
				        	
				        	//String tbName = bcNormalTbName.value();

				        	Long[] ips = bcIPArray.getValue();
			            	HashMap<Long,IPDatabase> ipMaps = bcIPMap.getValue();
			            	
			            	//得到源IP归属地信息
			            	Long lSourceIp = ApolloConfig.getStartIP(ips, sourceip);
			            	IPDatabase SourceArea = ipMaps.get(lSourceIp);
			            	if(SourceArea==null)
			            		return null;
			            	
			            	String sSourceInfo = String.format("%s|%s|%s|%s", 
			            			SourceArea.getCityName()==null?SourceArea.getCountryName():SourceArea.getCityName(),
			            		SourceArea.getCityID()==0?SourceArea.getCountryID():SourceArea.getCityID(),
			            		SourceArea.getLon(),SourceArea.getLat());
			            	
				        	String key = domainname + "|" + destip + "|" + destport 
				        			+ "|" + sSourceInfo;
				        	
				        	Tuple2<String, Integer> tuple = new Tuple2<String, Integer>(key, 1);
							return tuple;
						}
					}).filter(new Function<Tuple2<String, Integer>,Boolean>(){


						@Override
						public Boolean call(Tuple2<String, Integer> v1) throws Exception {
							// TODO Auto-generated method stub
							if(v1==null)
								return false;
							else
								return true;
						}
				    }).reduceByKey(
							new Function2<Integer, Integer, Integer>()
							{//计算每一个源->>>>>目标的IP得分
								
							

								@Override
								public Integer call(Integer v1, Integer v2) throws Exception {
									// TODO Auto-generated method stub
									return (v1 + v2);
								}
							}
							
						);
		
		
		//统计异常流量次数
		//正常流量入库
		Nomallines.foreach(new StreamInit(bcZooServer,bcDriverServer,
				broadcastColumnFamily, bcServerMap, 
				bcAbnSessStatHour, bcAbnSessStatDay));
		
//		Nomallines.foreach(new Function2<JavaPairRDD<String, Integer>, Time, Void>() {
//				@Override
//				public Void call(JavaPairRDD<String, Integer> v1, Time v2)
//							throws Exception {
//					final long time = v2.copy$default$1();
//						// TODO Auto-generated method stub
//					v1.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//
//						@Override
//				        public void call(Tuple2<String, Integer> tuple) throws Exception {
//				          
//							//初始化-----
//							//$$$$$流量统计--小时粒度
//							/*异常日志*/
//			            	HBaseRecordAdd addDestIP = HBaseRecordAdd.getInstance(
//			            			bcZooServer.getValue());
//							String cf = broadcastColumnFamily.value();
//							
//							//服务器信息
//							HashMap<String,ServerInfo> servers = bcServerMap.getValue();
//		                        
//		                    //异常流量统计表
//		                    String abnStatTbName = bcAbnSessStatHour.value();
//		                        
//		                    String[] items = tuple._1.split("\\|",-1);
//		                    String destip = items[1];
//		                    
//			            	SimpleDateFormat df1 = new SimpleDateFormat("yyyyMMddHH");
//							Date dStartTime1 = new Date(time);
//							String strDate1 = df1.format(dStartTime1);
//							
//							SimpleDateFormat dfHour = new SimpleDateFormat("HH");
//							Date dStartTimeH = new Date(time);
//							String strDateH = dfHour.format(dStartTimeH);
//							
//			            	String statRowKey = String.format("%s_%s", destip, strDate1);
//			            	addDestIP.incerment(abnStatTbName, statRowKey, "cf:PORTLOW", 0);
//			            	
//			            	
//			            	//addDestIP.Add(abnStatTbName, statRowKey, cf,  "DOMAIN", domainname);
//			            	addDestIP.Add(abnStatTbName, statRowKey, cf,  "IP", destip);
//			            	addDestIP.Add(abnStatTbName, statRowKey, cf,  "HOUR", strDateH);
//			            	
//			            	if(servers.containsKey(destip))
//			            	{
//			            		ServerInfo serinfo = servers.get(destip);
//			            		addDestIP.Add(abnStatTbName, statRowKey, cf, "ACCESSTYPE", serinfo.getAccessType());
//			            		addDestIP.Add(abnStatTbName, statRowKey, cf, "LOCATION", serinfo.getServerAddress());
//			            	}
//			            	else
//			            	{
//			            		addDestIP.Add(abnStatTbName, statRowKey, cf, "ACCESSTYPE", "NONE");
//			            		addDestIP.Add(abnStatTbName, statRowKey, cf, "LOCATION", "NONE");
//			            	}
//			            	
//			            	
//			            	//$$$$$异常流量统计--天粒度
//			            	String abnStatTbNameDay = bcAbnSessStatDay.value();
//			            	
//			            	SimpleDateFormat df2 = new SimpleDateFormat("yyyyMMdd");
//							Date dStartTime2 = new Date(time);
//							String strDate2 = df2.format(dStartTime2);
//			            	String statRowKeyD = String.format("%s_%s", destip, strDate2);
//			            	addDestIP.incerment(abnStatTbNameDay, statRowKeyD, "cf:PORTLOW", 0);
//			            	//addDestIP.Add(abnStatTbNameDay, statRowKeyD, cf,  "DOMAIN", domainname);
//			            	addDestIP.Add(abnStatTbNameDay, statRowKeyD, cf,  "IP", destip);
//			            	addDestIP.Add(abnStatTbNameDay, statRowKeyD, cf, "REPORTTIME", strDate2);
//
//			            	if(servers.containsKey(destip))
//			            	{
//			            		ServerInfo serinfo = servers.get(destip);
//			            		addDestIP.Add(abnStatTbNameDay, statRowKeyD, cf, "ACCESSTYPE", serinfo.getAccessType());
//			            		addDestIP.Add(abnStatTbNameDay, statRowKeyD, cf, "LOCATION", serinfo.getServerAddress());
//			            	}
//			            	else
//			            	{
//			            		addDestIP.Add(abnStatTbNameDay, statRowKeyD, cf, "ACCESSTYPE", "NONE");
//			            		addDestIP.Add(abnStatTbNameDay, statRowKeyD, cf, "LOCATION", "NONE");
//			            	}
//						}
//					});
//
//				    return null;
//				}
//		});
		
		//异常流量入库
		AbNomallines.foreach(new AbNormalStream(bcZooServer, bcDriverServer,
				bcServerMap, bcAbnormalTbName, 
				broadcastColumnFamily, 
				bcAbnSessStatHour, bcAbnSessStatDay, 
				bcPorts, bcIPArray, bcIPMap, bcAbnSessSourHour, 
				bcAbnSessSourDay));
		
		//异常流量分布情况
		AbNomallines.mapToPair(new PairFunction<Tuple2<String, Integer>,String, Integer>() {
			

				@Override
				public Tuple2<String, Integer> call(Tuple2<String, Integer> t)
						throws Exception {
					// TODO Auto-generated method stub
						/*destip + "|" + destport + "|" + sourceip 
		        			+ "|" + score + "|" + desc;*/
						String[] items = t._1.split("\\|",-1);
		         		
			        	//String domainname = items[0];
		         		String destip = items[0];
		         		//String destport = items[1];
			        	String sourceip = items[2];
			        	
			        	//**IP归属地信息匹配
		            	Long[] ips = bcIPArray.getValue();
		            	HashMap<Long,IPDatabase> ipMaps = bcIPMap.getValue();
		            	
		            	Long lSourceIp = ApolloConfig.getStartIP(ips, sourceip);
		            	IPDatabase SourceArea = ipMaps.get(lSourceIp);
		            	String sSourceArea = String.format("%s",
		            			SourceArea.getProviceName()==null?SourceArea.getCountryName():SourceArea.getProviceName().replace("省", ""));
		            	String areaID = String.format("%d", SourceArea.getProviceID()==0?SourceArea.getCountryID():SourceArea.getProviceID());
		            	String sKey = destip + "_" + sSourceArea + "_" + areaID;
		            	
		        	Tuple2<String, Integer> tuple = new Tuple2<String, Integer>(sKey, 1);
					return tuple;
				}
			}).reduceByKey(new Function2<Integer, Integer, Integer>()
				{//统计来源归属地
				

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						// TODO Auto-generated method stub
						return (v1 + v2);
					}
					
				}).foreach(new Function2<JavaPairRDD<String, Integer>, Time, Void>() {
					

					@Override
					public Void call(JavaPairRDD<String, Integer> v1, Time v2)
							throws Exception {
						final long time = v2.copy$default$1();
						// TODO Auto-generated method stub
						v1.foreach(new VoidFunction<Tuple2<String, Integer>>() {
							  

							@Override
				              public void call(Tuple2<String, Integer> tuple) throws Exception {
				                
				            	/*服务器评估*/
				            	HBaseRecordAdd addArea = HBaseRecordAdd.getInstance(bcZooServer.getValue(),bcDriverServer.getValue());
				         		
				         		/*获取相关配置信息*/
				         		
				         		HashMap<String,ServerInfo> servers = bcServerMap.getValue();
		
				         		String key = tuple._1;
				         		String items[] = key.split("_",-1);
				         		
					        	 
				         		String tbName = bcAreaTbName.value();
		                        String cf = broadcastColumnFamily.value();
		                        
		                        //String domainname = items[0];
		                        String destip = items[0];
		                        String area = items[1];
		                        String areaID = items[2];
		                        
					        	SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
								Date dStartTime = new Date(time);
								String strDate = df.format(dStartTime);
				         		 
				            	String rowkey = String.format("%s_%s_%s", destip, strDate, areaID);
				            	addArea.Add(tbName, rowkey, cf,  "REPORTTIME", strDate);
				            	addArea.Add(tbName, rowkey, cf,  "IP", destip);
				            	addArea.Add(tbName, rowkey, cf,  "AREA", area);
				            	addArea.incerment(tbName, rowkey, "cf:PV", tuple._2);
				            	
				            	ServerInfo server = servers.get(destip);
					        	if(server != null)
					        		addArea.Add(tbName, rowkey, cf,  "SERVERID", String.valueOf(server.getServerID()));
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

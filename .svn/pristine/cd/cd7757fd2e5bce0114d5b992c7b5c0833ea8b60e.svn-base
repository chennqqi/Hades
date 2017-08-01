package com.aotain.project.apollo.port;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Time;

import scala.Tuple2;

import com.aotain.hbase.dataimport.HBaseRecordAdd;
import com.aotain.project.apollo.ApolloConfig;
import com.aotain.project.apollo.IPDatabase;
import com.aotain.project.apollo.PortInfo;
import com.aotain.project.apollo.ServerInfo;

/**
 * 端口的异常流量处理方法，Spark Streaming
 * 被PortDetect类使用
 * @author Administrator
 *
 */
public class AbNormalStream 
	implements Function2<JavaPairRDD<String, Integer>, Time, Void>{

	/**
	 * 服务器配置
	 */
	Broadcast<HashMap<String,ServerInfo>> bcServerMap = null;
	/**
	 * ZooServer
	 */
	Broadcast<String> bcZooServer = null;
	
	Broadcast<String> bcDriverServer = null;
	
	/**
	 * 异常日志表名
	 */
	Broadcast<String> bcAbnormalTbName = null;
	
	/**
	 * 列簇名
	 */
	Broadcast<String> broadcastColumnFamily = null;
	
	/**
	 * 异常流量统计小时粒度表名
	 */
	Broadcast<String> bcAbnSessStatHour = null;
	
	/**
	 * 异常流量统计天粒度表名
	 */
	Broadcast<String> bcAbnSessStatDay = null;
	
	/**
	 * 检测端口配置
	 */
	Broadcast<HashMap<Integer,PortInfo>> bcPorts = null;
	
	/**
	 * IP库数组
	 */
	Broadcast<Long[]> bcIPArray = null;
	
	/**
	 * IP库对象MAP
	 */
	Broadcast<HashMap<Long,IPDatabase>> bcIPMap = null;
	
	/**
	 * 异常流量源IP统计小时粒度表名
	 */
	Broadcast<String> bcAbnSessSourHour = null;
	
	/**
	 * 异常流量IP统计天粒度表名
	 */
	Broadcast<String> bcAbnSessSourDay = null;
	
			
	/**
	 * 端口的异常流量处理方法，Spark Streaming
	 * @param parabcZooServer ZooServer
	 * @param parabcServerMap 服务器配置
	 * @param parabcAbnormalTbName 异常日志表名
	 * @param paraColumnFamily 列簇名
	 * @param parabcAbnSessStatHour  异常流量统计小时粒度表名
	 * @param parabcAbnSessStatDay  异常流量统计天粒度表名
	 * @param parabcPorts 检测端口配置
	 * @param parabcIPArray IP库数组
	 * @param parabcIPMap IP库对象MAP
	 * @param parabcAbnSessSourHour 异常流量源IP统计小时粒度表名
	 * @param parabcAbnSessSourDay  异常流量IP统计天粒度表名
	 */
	public AbNormalStream(Broadcast<String> parabcZooServer,
			Broadcast<String> parabcDriverServer,
			Broadcast<HashMap<String,ServerInfo>> parabcServerMap,
			Broadcast<String> parabcAbnormalTbName,
			Broadcast<String> paraColumnFamily,
			Broadcast<String> parabcAbnSessStatHour,
			Broadcast<String> parabcAbnSessStatDay,
			Broadcast<HashMap<Integer,PortInfo>> parabcPorts,
			Broadcast<Long[]> parabcIPArray,
			Broadcast<HashMap<Long,IPDatabase>> parabcIPMap,
			Broadcast<String> parabcAbnSessSourHour,
			Broadcast<String> parabcAbnSessSourDay)
	{
		bcZooServer = parabcZooServer;
		bcDriverServer = parabcDriverServer;
		bcServerMap = parabcServerMap;
		bcAbnormalTbName = parabcAbnormalTbName;
		broadcastColumnFamily = paraColumnFamily;
		bcAbnSessStatHour = parabcAbnSessStatHour;
		bcAbnSessStatDay = parabcAbnSessStatDay;
		bcPorts = parabcPorts;
		bcIPArray = parabcIPArray;
		bcIPMap = parabcIPMap;
		bcAbnSessSourHour = parabcAbnSessSourHour;
		bcAbnSessSourDay = parabcAbnSessSourDay;
	}

	@Override
	public Void call(JavaPairRDD<String, Integer> v1, Time v2) throws Exception {
		// TODO Auto-generated method stub
		final long time = v2.copy$default$1();
		
		// TODO Auto-generated method stub
		v1.foreach(new VoidFunction<Tuple2<String, Integer>>() {

			@Override
              public void call(Tuple2<String, Integer> tuple) throws Exception {
                
            	/*异常日志*/
            	HBaseRecordAdd addDestIP = HBaseRecordAdd.getInstance(
            			bcZooServer.getValue(),bcDriverServer.getValue());
         		String[] items = tuple._1.split("\\|",-1);
         		
         		/*获取相关配置信息*/
         		HashMap<String,ServerInfo> servers = bcServerMap.getValue();
         		
         		
	        	/*destip + "|" + destport + "|" + sourceip 
        			+ "|" + score + "|" + desc;*/
         		//String domainname = items[0];
         		String destip = items[0];
         		String destport = items[1];
	        	String sourceip = items[2];
	        	String score = items[3];
	        	
	        	String tbName = bcAbnormalTbName.value();
                String cf = broadcastColumnFamily.value();
                
                //异常流量统计表
                String abnStatTbName = bcAbnSessStatHour.value();
                
                String abnStatTbNameDay = bcAbnSessStatDay.value();
	        	
	        
	        	SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
				Date dStartTime = new Date(time);
				String strDate = df.format(dStartTime);
         		
            	String  rowkey = String.format("%s_%s_%s_%s", destip, strDate, sourceip,destport);
            	addDestIP.Add(tbName, rowkey, cf,  "DESTIP", destip);
            	addDestIP.Add(tbName, rowkey, cf,  "DESTPORT", destport);
            	addDestIP.Add(tbName, rowkey, cf,  "SOURCEIP", sourceip);
            	//addDestIP.Add(tbName, rowkey, cf,  "DOMAIN", domainname);
            	//addDestIP.Add(rowkey, "SOURCEPORT", sourceport);
            	addDestIP.Add(tbName, rowkey, cf, "ABRNORMAL", "4");
            	addDestIP.Add(tbName, rowkey, cf,  "EVALUATE", score);
            	
            	HashMap<Integer,PortInfo> postMap = bcPorts.getValue();
            	PortInfo portinfo = postMap.get(Integer.parseInt(destport));
            	addDestIP.Add(tbName, rowkey, cf,  "DESC", portinfo.Desc);
            	addDestIP.Add(tbName, rowkey, cf,  "ATTNUM", String.valueOf(tuple._2));
            	
            	ServerInfo server = servers.get(destip);
	        	if(server != null)
	        		addDestIP.Add(tbName, rowkey, cf,  "SERVERID", String.valueOf(server.getServerID()));
	        	
            	//**IP归属地信息匹配
            	Long[] ips = bcIPArray.getValue();
            	HashMap<Long,IPDatabase> ipMaps = bcIPMap.getValue();
            	
            	Long lSourceIp = ApolloConfig.getStartIP(ips, sourceip);
            	IPDatabase SourceArea = ipMaps.get(lSourceIp);
            	if(SourceArea!=null)
            	{
            		addDestIP.Add(tbName, rowkey, cf,  "SOURCEAREA", String.format("%s",
            			SourceArea.getCityName() == null?SourceArea.getCountryName():SourceArea.getCityName().replace("市", "")));
            		
            		
            		addDestIP.Add(tbName, rowkey, cf,  "SOURCEGEO", String.format("%s,%s",
	            			SourceArea.getLon(),SourceArea.getLat()));
	            		
            	}
            	
            	Long lDestIp = ApolloConfig.getStartIP(ips, destip);
            	IPDatabase DestArea = ipMaps.get(lDestIp);
            	if(DestArea!=null)
            	{
            		addDestIP.Add(tbName, rowkey, cf,  "DESTAREA", String.format("%s",
            			DestArea.getCityName()==null?DestArea.getCountryName():DestArea.getCityName().replace("市", "")));
            		
            		addDestIP.Add(tbName, rowkey, cf,  "DESTGEO", String.format("%s,%s",
            			DestArea.getLon(),DestArea.getLat()));
	            		
            	}
            	
            	
            	
            	//$$$$$异常流量统计--小时粒度
            	SimpleDateFormat df1 = new SimpleDateFormat("yyyyMMddHH");
				Date dStartTime1 = new Date(time);
				String strDateHour = df1.format(dStartTime1);
				
				SimpleDateFormat dfHour = new SimpleDateFormat("HH");
				Date dStartTimeH = new Date(time);
				String strDateH = dfHour.format(dStartTimeH);
				
				
            	String statRowKey = String.format("%s_%s", destip, strDateHour);
            	addDestIP.incerment(abnStatTbName, statRowKey, "cf:PORTLOW", 1);
            	addDestIP.Add(abnStatTbName, statRowKey, cf, "REPORTTIME", strDateHour);
            	//addDestIP.Add(abnStatTbName, statRowKey, cf,  "DOMAIN", domainname);
            	addDestIP.Add(abnStatTbName, statRowKey, cf,  "IP", destip);
//            	addDestIP.Add(abnStatTbName, statRowKey, cf, "ACCESSTYPE", "虚拟主机");
//            	addDestIP.Add(abnStatTbName, statRowKey, cf, "LOCATION", "上海");
            	addDestIP.Add(abnStatTbName, statRowKey, cf, "HOUR", strDateH);
            	if(servers.containsKey(destip))
            	{
            		ServerInfo serinfo = servers.get(destip);
            		addDestIP.Add(abnStatTbName, statRowKey, cf, "ACCESSTYPE", serinfo.getAccessType());
            		addDestIP.Add(abnStatTbName, statRowKey, cf, "LOCATION", serinfo.getServerAddress());
            	}
            	else
            	{
            		addDestIP.Add(abnStatTbName, statRowKey, cf, "ACCESSTYPE", "NONE");
            		addDestIP.Add(abnStatTbName, statRowKey, cf, "LOCATION", "NONE");
            	}
            	
            	//---异常流量来源统计--小时
            	String abnSourTbName = bcAbnSessSourHour.getValue();
            	String statSourRowKey = String.format("%s_%s_%s", destip,strDateHour,sourceip);
            	addDestIP.incerment(abnSourTbName, statSourRowKey, "cf:PV", 1);
            	//addDestIP.Add(abnSourTbName, statSourRowKey, cf,  "DOMAIN", domainname);
            	addDestIP.Add(abnSourTbName, statSourRowKey, cf,  "DESTIP", destip);
            	addDestIP.Add(abnSourTbName, statSourRowKey, cf,  "SOURCEIP", sourceip);
            	addDestIP.Add(abnSourTbName, statSourRowKey, cf, "REPORTTIME", strDateHour);
            	if(SourceArea!=null)
            	{
            		addDestIP.Add(abnSourTbName, statSourRowKey, cf,  "SOURCEAREA", String.format("%s",
            			SourceArea.getCityName() == null?SourceArea.getCountryName():SourceArea.getCityName().replace("市", "")));
            	}
            	
            	
            	//$$$$$异常流量统计--天粒度
            	SimpleDateFormat df2 = new SimpleDateFormat("yyyyMMdd");
				Date dStartTime2 = new Date(time);
				String strDateDay = df2.format(dStartTime2);
            	String statRowKeyD = String.format("%s_%s", destip, strDateDay);
            	addDestIP.incerment(abnStatTbNameDay, statRowKeyD, "cf:PORTLOW", 1);
            	
            	addDestIP.Add(abnStatTbNameDay, statRowKeyD, cf,  "IP", destip);
            	addDestIP.Add(abnStatTbNameDay, statRowKeyD, cf, "REPORTTIME", strDateDay);
//            	addDestIP.Add(abnStatTbNameDay, statRowKeyD, cf, "ACCESSTYPE", "虚拟主机");
//            	addDestIP.Add(abnStatTbNameDay, statRowKeyD, cf, "LOCATION", "上海");
            	if(servers.containsKey(destip))
            	{
            		ServerInfo serinfo = servers.get(destip);
            		addDestIP.Add(abnStatTbNameDay, statRowKeyD, cf, "ACCESSTYPE", serinfo.getAccessType());
            		addDestIP.Add(abnStatTbNameDay, statRowKeyD, cf, "LOCATION", serinfo.getServerAddress());
            	}
            	else
            	{
            		addDestIP.Add(abnStatTbNameDay, statRowKeyD, cf, "ACCESSTYPE", "NONE");
            		addDestIP.Add(abnStatTbNameDay, statRowKeyD, cf, "LOCATION", "NONE");
            	}
            	
            	
            	//---异常流量来源统计--天
            	String abnSourTbNameDay = bcAbnSessSourDay.getValue();
            	String statSourRowKeyDay = String.format("%s_%s_%s", destip,strDateDay,sourceip);
            	addDestIP.incerment(abnSourTbNameDay, statSourRowKey, "cf:PV", 1);
            	//addDestIP.Add(abnSourTbNameDay, statSourRowKeyDay, cf,  "DOMAIN", domainname);
            	addDestIP.Add(abnSourTbNameDay, statSourRowKeyDay, cf,  "DESTIP", destip);
            	addDestIP.Add(abnSourTbNameDay, statSourRowKeyDay, cf,  "SOURCEIP", sourceip);
            	addDestIP.Add(abnSourTbNameDay, statSourRowKeyDay, cf, "REPORTTIME", strDateDay);
            	if(SourceArea!=null)
            	{
            		addDestIP.Add(abnSourTbNameDay, statSourRowKeyDay, cf,  "SOURCEAREA", String.format("%s",
            			SourceArea.getCityName() == null?SourceArea.getCountryName():SourceArea.getCityName().replace("市", "")));
            	}
            	//Logger.getRootLogger().info("AbNomallines Add************************");
            	
            	
              }
            });

		return null;
	}

}

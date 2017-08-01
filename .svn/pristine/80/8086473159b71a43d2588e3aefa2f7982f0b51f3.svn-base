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
import com.aotain.project.apollo.ServerInfo;

public class StreamInit implements Function2<JavaPairRDD<String, Integer>, Time, Void> {
	
	Broadcast<String> bcZooServer = null;
	Broadcast<String> bcDriverServer = null;
	Broadcast<String> broadcastColumnFamily = null;
	Broadcast<HashMap<String,ServerInfo>> bcServerMap = null;
	Broadcast<String> bcAbnSessStatHour = null;
	Broadcast<String> bcAbnSessStatDay = null;
	
	public StreamInit(Broadcast<String> parabcZooServer,
			Broadcast<String> parabcDriverServer,
			Broadcast<String> parabcColumnFamily,
			Broadcast<HashMap<String,ServerInfo>> parabcServerMap,
			Broadcast<String> parabcAbnSessStatHour,
			Broadcast<String> parabcAbnSessStatDay)
	{
		bcZooServer = parabcZooServer;
		bcDriverServer = parabcDriverServer;
		broadcastColumnFamily = parabcColumnFamily;
		bcServerMap = parabcServerMap;
		bcAbnSessStatHour = parabcAbnSessStatHour;
		bcAbnSessStatDay = parabcAbnSessStatDay;
		
	}

	@Override
	public Void call(JavaPairRDD<String, Integer> v1, Time v2) throws Exception {
		// TODO Auto-generated method stub
		
		final long time = v2.copy$default$1();
		// TODO Auto-generated method stub
	v1.foreach(new VoidFunction<Tuple2<String, Integer>>() {

		@Override
        public void call(Tuple2<String, Integer> tuple) throws Exception {
          
			//初始化-----
			//$$$$$流量统计--小时粒度
			/*异常日志*/
        	HBaseRecordAdd addDestIP = HBaseRecordAdd.getInstance(
        			bcZooServer.getValue(),bcDriverServer.getValue());
			String cf = broadcastColumnFamily.value();
			
			//服务器信息
			HashMap<String,ServerInfo> servers = bcServerMap.getValue();
                
            //异常流量统计表
            String abnStatTbName = bcAbnSessStatHour.value();
                
            String[] items = tuple._1.split("\\|",-1);
            String destip = items[1];
            
        	SimpleDateFormat df1 = new SimpleDateFormat("yyyyMMddHH");
			Date dStartTime1 = new Date(time);
			String strDate1 = df1.format(dStartTime1);
			
			SimpleDateFormat dfHour = new SimpleDateFormat("HH");
			Date dStartTimeH = new Date(time);
			String strDateH = dfHour.format(dStartTimeH);
			
        	String statRowKey = String.format("%s_%s", destip, strDate1);
        	addDestIP.incerment(abnStatTbName, statRowKey, "cf:PORTLOW", 0);
        	
        	
        	//addDestIP.Add(abnStatTbName, statRowKey, cf,  "DOMAIN", domainname);
        	addDestIP.Add(abnStatTbName, statRowKey, cf,  "IP", destip);
        	addDestIP.Add(abnStatTbName, statRowKey, cf,  "HOUR", strDateH);
        	
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
        	
        	
        	//$$$$$异常流量统计--天粒度
        	String abnStatTbNameDay = bcAbnSessStatDay.value();
        	
        	SimpleDateFormat df2 = new SimpleDateFormat("yyyyMMdd");
			Date dStartTime2 = new Date(time);
			String strDate2 = df2.format(dStartTime2);
        	String statRowKeyD = String.format("%s_%s", destip, strDate2);
        	addDestIP.incerment(abnStatTbNameDay, statRowKeyD, "cf:PORTLOW", 0);
        	//addDestIP.Add(abnStatTbNameDay, statRowKeyD, cf,  "DOMAIN", domainname);
        	addDestIP.Add(abnStatTbNameDay, statRowKeyD, cf,  "IP", destip);
        	addDestIP.Add(abnStatTbNameDay, statRowKeyD, cf, "REPORTTIME", strDate2);

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
		}
	});
	
		return null;
	}

}

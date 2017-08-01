package com.aotain.project.apollo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;

import com.aotain.common.CommonDB;
import com.aotain.common.CommonFunction;
import com.aotain.common.DBConfigInit;
import com.aotain.common.DbPool;

public class ApolloConfig {

	DBConfigInit db = null;
	
	private Long[] IPs = new Long[]{};
			
	public ApolloConfig(String dbconfig)
	{
		db = new DBConfigInit(dbconfig);
	}
	
	
	
	/**
	 * 获取IP归属地配置
	 * @return
	 */
	public HashMap<Long,IPDatabase> IPDataBaseMap()
	{
		ResultSet rs = null;
		Connection con = null;
		PreparedStatement ps = null;
		HashMap<Long,IPDatabase> map = new HashMap();
		int count = 0;
		try
		{
			con = DbPool.getConn();
			ps=con.prepareStatement("select start_ip,"
				       +"end_ip,"
				       +"c.position_id      as country_id,"
				       +"c.position_name    as country_name,"
				       +"p.position_id      as province_id,"
				       +"p.position_name    as province_name,"
				       +"city.position_id   as city_id,"
				       +"city.position_name as city_name,"
				       +"case when city.position_name is null then c.lon else city.lon end lon,"
				       +"case when city.position_name is null then c.lat else city.lat end lat"
					  +" from dic_ip_db a,"
					  +"   (select c1.position_id, position_name,g1.lon,g1.lat"
					  +"      from dic_sys_position c1,dic_geo_info g1"
					  +"     where position_type = 1 and c1.position_id = g1.position_id(+)) c,"
					  +"   (select p1.position_id, position_name,g2.lon,g2.lat"
					  +"      from dic_sys_position p1,dic_geo_info g2"
					  +"     where position_type = 2 and p1.position_id = g2.position_id(+)) p,"
					  +"   (select city1.position_id, position_name,g3.lon,g3.lat"
					  +"      from dic_sys_position city1,dic_geo_info g3"
					  +"     where (position_type = 3 or position_type = 2) and city1.position_id = g3.position_id(+)"
					  +"        ) city"
					 +" where a.country_id = c.position_id"
					   +" and a.province_id = p.position_id(+)"
					  +" and a.city_id = city.position_id(+)");
				       
			rs=ps.executeQuery();
			IPs = new Long[rs.getRow()];
	        while(rs.next()){
	        	IPDatabase ip = new IPDatabase();
	        	ip.setStartIP(CommonFunction.ip2int(rs.getString("start_ip")));
	        	ip.setEndIP(CommonFunction.ip2int(rs.getString("end_ip")));
	        	ip.setCountryName(rs.getString("country_name"));
	        	ip.setProviceName(rs.getString("province_name"));
	        	ip.setCityName(rs.getString("city_name"));
	        	
	        	ip.setCountryID(rs.getInt("country_id"));
	        	ip.setProviceID(rs.getInt("province_id"));
	        	ip.setCityID(rs.getInt("city_id"));
	        	
	        	ip.setLon(rs.getFloat("lon"));
	        	ip.setLat(rs.getFloat("lat"));
	        	
	        	map.put(ip.getStartIP(), ip);
	        	count++;
	        	
	        	
	        }
	        IPs = new Long[count];
	        count = 0;
	        for(Long ip : map.keySet())
	        {
	        	IPs[count] = ip;
	        	count++;
	        }
	        
	        
		}
		catch(Exception ex)
		{
			System.out.println("getKeywordConfig:"+ex.getMessage());
		}
		finally
		{
			CommonDB.closeDBConnection(con, ps, rs);
		}
		//BubbleSort();//对于key做一次排序，方便后面查找使用
		sort();
		return map;
	}
	
	/**
	 * 获取IPs
	 * @return
	 */
	public Long[] StartIPs()
	{
		return IPs;
	}
	
	private void BubbleSort()
	{
		  //for (int i = 0; i < IPs.length; i++) {
		//	  System.out.print(IPs[i]+" ");
		  //}
		  //冒泡排序
		  for (int i = 0; i < IPs.length; i++) {
			  for(int j = 0; j<IPs.length-i-1; j++){
				  //这里-i主要是每遍历一次都把最大的i个数沉到最底下去了，没有必要再替换了
				  if(IPs[j]>IPs[j+1]){
					  Long temp = IPs[j];
					  IPs[j] = IPs[j+1];
					  IPs[j+1] = temp;
				  }
			  }
		  }
	}
	
	private void sort() {
		for (int i = 0; i < IPs.length; i++) {
			Long temp = IPs[i];
			int left = 0;
			int right = i-1;
			int mid = 0;
			while(left<=right){
				mid = (left+right)/2;
				if(temp<IPs[mid]){
					right = mid-1;
				}else{
					left = mid+1;
				}
			}
			for (int j = i-1; j >= left; j--) {
				IPs[j+1] = IPs[j];
			}
			if(left != i){
				IPs[left] = temp;
			}
		}
	}
	
	public static Long getStartIP(Long[] IPs,String findIp)
	{
		Long ip = 0L;
		Long searchIP = CommonFunction.ip2int(findIp);
		int start = 0;  
        int end= IPs.length - 1; 
        int middle = 0;
        Long middleValue = 0l;
        while(start<=end)  
        { 
            //中间位置  
            middle = (start+end)/2;    //相当于(start+end)/2  
            //中值  
            middleValue = IPs[middle];  
              
            if(searchIP == middleValue)  
            {  
                //等于中值直接返回  
                return middleValue;  
            }  
            else if( searchIP < middleValue)  
            {  
                //小于中值时在中值前面找  
                end=middle-1;  
            }  
            else  
            {  
                //大于中值在中值后面找  
                start = middle + 1;
            }  
        } 
        
        //一般情况不能直接找到,在搜索完成后，当前的中值位置，中值，记录下来
        if(searchIP > middleValue)
        {
        	ip = middleValue;
        }
        else
        {
        	if(middle > IPs.length)
        	{
        		ip = IPs[IPs.length];
        	}
        	else
        	{
        		ip = IPs[middle - 1];
        	}
        }
		
		return ip;
	}
	
	/**
	 * 加载服务器配置信息
	 * @return
	 */
	public HashMap<String,ServerInfo> ServerInfos()
	{
		HashMap<String,ServerInfo> map = new HashMap<String,ServerInfo>();
		
		ResultSet rs = null;
		Connection con = null;
		PreparedStatement ps = null;
		try
		{
			
			con = CommonDB.getConnection();
			
			ps=con.prepareStatement("select serverid,a.siteid,a.ip,area,serveraddress,accesstype "
					+ "from SDS_SERVERINFO A JOIN SDS_WEBSITEINFO B ON A.SITEID = B.SITEID");
			rs=ps.executeQuery();
			
	        while(rs.next()){
	        	ServerInfo server = new ServerInfo();
	        	server.setServerID(rs.getLong("serverid"));
	        	server.setSiteID(rs.getLong("siteid"));
	        	server.setIP(rs.getString("ip"));
	        	server.setArea(rs.getString("area"));
	        	server.setAccessType(rs.getString("accesstype"));
	        	server.setServerAddress(rs.getString("serveraddress"));
	        	map.put(server.getIP(), server);
	        }
		}
		catch(Exception ex)
		{
			System.out.println("getServerInfos:"+ex.getMessage());
		}
		finally
		{
			CommonDB.closeDBConnection(con, ps, rs);
		}
		
		return map;
	}
	
	public HashMap<String,String> CheckIPs()
	{
		HashMap<String,String> map = new HashMap<String,String>();
		
		String ips = db.getIPs();
		String[] items = ips.split(",",-1);
		for(String ip:items)
		{
			map.put(ip, ip);
		}
		
		return map;
	}
}

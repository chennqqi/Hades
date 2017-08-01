package com.aotain.project.apollo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.aotain.common.CommonDB;

/**
 * 从数据库中读取端口免疫配置
 * @author Administrator
 *
 */
public class PortImmune {
	
	private HashMap<String,HashMap<Integer,Integer>> _portMap = new HashMap();
	
	public PortImmune()
	{
		ResultSet rs = null;
		Connection con = null;
		PreparedStatement ps = null;
		
		try
		{
			Logger.getRootLogger().info("Load SDS_MOD_PORTIMMUNE start");
			con = CommonDB.getConnection();
			String strSql = String.format("SELECT IP,PORT FROM SDS_MOD_PORTIMMUNE");
			ps=con.prepareStatement(strSql);
			rs=ps.executeQuery();
			while(rs.next())
			{
				String IP = rs.getString("IP");
				String ports = rs.getString("PORT");
				
				if(_portMap.containsKey(IP))
				{
					HashMap<Integer,Integer> portMap = _portMap.get(IP);
					String[] arrPort = ports.split(",",-1);
					
					for(String port : arrPort)
					{
						portMap.put(Integer.parseInt(port), Integer.parseInt(port));
					}
				}
				else
				{
					HashMap<Integer,Integer> portMap = new HashMap();
					String[] arrPort = ports.split(",",-1);
					
					for(String port : arrPort)
					{
						portMap.put(Integer.parseInt(port), Integer.parseInt(port));
					}
					
					_portMap.put(IP, portMap);
				}
				
			}
			
			Logger.getRootLogger().info("Load SDS_MOD_PORTIMMUNE end");
		}
		catch(Exception ex)
		{
			Logger.getRootLogger().error("Load SDS_MOD_PORTIMMUNE Error:",ex);
		}
	}
	
	public HashMap<String,HashMap<Integer,Integer>> getPortImmune()
	{
		return this._portMap;
	}

}

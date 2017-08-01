package com.aotain.project.apollo;

import java.io.Serializable;

public class ServerInfo implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -9125260646092572216L;
	private long _serverid;
	private long _siteid;
	private String _ip;
	private String _area;
	private String _serveraddress;
	private String _accesstype;
	
	public long getServerID()
	{
		return this._serverid;
	}
	
	public void setServerID(long serverid)
	{
		this._serverid = serverid;
	}
	
	public long getSiteID()
	{
		return this._siteid;
	}
	
	public void setSiteID(long siteid)
	{
		this._siteid = siteid;
	}
	
	public String getIP()
	{
		return this._ip;
	}
	
	public void setIP(String ip)
	{
		this._ip = ip;
	}
	
	public String getArea()
	{
		return this._area;
	}
	
	public void setArea(String area)
	{
		this._area = area;
	}
	
	public String getServerAddress()
	{
		return this._serveraddress;
	}
	
	/**
	 * 托管地址
	 * @param serveraddress
	 */
	public void setServerAddress(String serveraddress)
	{
		this._serveraddress = serveraddress;
	}
	
	public String getAccessType()
	{
		return this._accesstype;
	}
	
	/**
	 * 接入类型
	 * @param accesstype
	 */
	public void setAccessType(String accesstype)
	{
		this._accesstype = accesstype;
	}
}


package com.aotain.project.apollo;

import java.io.Serializable;

public class IPDatabase implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -2723494118145019966L;
	/*
	 *  START_IP
		END_IP
		COUNTRY_NAME
		PROVINCE_NAME
		CITY_NAME
	 */
	private Long _startip;
	private Long _endip;
	private String _countryname;
	private String _provicename;
	private String _cityname;
	
	private int _countryid;
	private int _proviceid;
	private int _cityid;
	
	private float _lon;
	private float _lat;
	
	public void setStartIP(Long startip)
	{
		this._startip = startip;
	}
	
	public Long getStartIP()
	{
		return this._startip;
	}
	
	public void setEndIP(Long endip)
	{
		this._endip = endip;
	}
	
	public Long getEndIP()
	{
		return this._endip;
	}
	
	public void setCountryID(int countryid)
	{
		this._countryid = countryid;
	}
	
	public int getCountryID()
	{
		return this._countryid;
	}
	
	public void setCountryName(String countryname)
	{
		this._countryname = countryname;
	}
	
	public String getCountryName()
	{
		return this._countryname;
	}
	
	public void setProviceName(String provicename)
	{
		this._provicename = provicename;
	}
	
	public String getProviceName()
	{
		return this._provicename;
	}
	
	public void setProviceID(int proviceid)
	{
		this._proviceid = proviceid;
	}
	
	public int getProviceID()
	{
		return this._proviceid;
	}
	
	
	public void setCityName(String cityname)
	{
		this._cityname = cityname;
	}
	
	public String getCityName()
	{
		return this._cityname;
	}
	
	public void setCityID(int cityid)
	{
		this._cityid = cityid;
	}
	
	public int getCityID()
	{
		return this._cityid;
	}
	
	public void setLon(float lon)
	{
		this._lon = lon;
	}
	
	public float getLon()
	{
		return this._lon;
	}
	
	public void setLat(float lat)
	{
		this._lat = lat;
	}
	
	public float getLat()
	{
		return this._lat;
	}
	
	
	
	
}

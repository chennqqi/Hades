package com.aotain.hbase.dataimport;

/**
 * HBASE KV
 * @author Administrator
 *
 */
public class HKeyValue {
	
	public HKeyValue(String columnfamily,String key,String value)
	{
		this._columnfamily = columnfamily;
		this._key = key;
		this._value = value;
	}

	private String _columnfamily;
	private String _key;
	private String _value;
	
	private long _time;
	

	public void setKey(String key)
	{
		this._key = key;
	}
	
	public String getKey()
	{
		return this._key;
	}
	
	public void setValue(String value)
	{
		this._value = value;
	}
	
	public String getValue()
	{
		return this._value;
	}
	
	public void setColumnFamily(String columnfamily)
	{
		this._columnfamily = columnfamily;
	}
	
	public String getColumnFamily()
	{
		return this._columnfamily;
	}
	
	public void setTime(long time)
	{
		this._time = time;
	}
	
	public long getTime()
	{
		return this._time;
	}
}

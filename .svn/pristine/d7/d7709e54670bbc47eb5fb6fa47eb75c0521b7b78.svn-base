package com.aotain.mushroom;

import net.sf.json.JSONObject;


public class HBaseImportLog extends AbstractMsg{
	
	private String _tablename;
	private int _addnum;
	private int _increnum;
	private String _servername;
	
	public String getTableName()
	{
		return _tablename;
	}
	
	public void setTableName(String tablename)
	{
		this._tablename = tablename;
	}
	
	public int getAddNum()
	{
		return _addnum;
	}
	
	public void setAddNum(int addnum)
	{
		this._addnum = addnum;
	}
	
	public int getIncreNum()
	{
		return this._increnum;
	}
	
	public void setIncreNum(int increnum)
	{
		this._increnum = increnum;
	}
	
	public String getServerName()
	{
		return this._servername;
	}
	
	public void setServerName(String servername)
	{
		this._servername = servername;
	}
	
	

	public HBaseImportLog getByJson(String json)
	{
		JSONObject jsonobject = JSONObject.fromObject(json);
		HBaseImportLog register = null;
		register = (HBaseImportLog)JSONObject.toBean(jsonobject,
				HBaseImportLog.class);
		
		return register;
	}
}

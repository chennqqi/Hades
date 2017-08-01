package com.aotain.mushroom;

import java.sql.Connection;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.aotain.common.CommonDB;
import com.aotain.common.DbPool;

/**
 * 蘑菇仓库
 * 存放所有需要driver 服务器处理的方法
 * @author Administrator
 *
 */
public class MushroomWarehouse {

	private static MushroomWarehouse _instance;
	
	public MushroomWarehouse()
	{
		
	}
	
	public static MushroomWarehouse getInstance()
	{
		if(_instance == null)
			_instance = new MushroomWarehouse();
		return _instance;
	}
	
	
	/**
	 * 写入
	 * @param TableName
	 * @param AddNum
	 * @param IncreNum
	 */
	public void InsertHBaseImportLog(HBaseImportLog msg)
	{
		
		Statement st = null;
		String insert = "insert into SDS_HBASEIMPORT_LOG " +
				"(STAMPTIME,TABLENAME,SERVERNAME,ADDNUM,INCREMENTNUM) " +
				"values (sysdate,'%s','%s',%d,%d)";
		Connection con = null;
		try
		{
			con = DbPool.getConn();
			
			con.setAutoCommit(false);
			st = con.createStatement();
			
			

			insert = String.format(insert, new Object[] {msg.getTableName(),msg.getServerName(),
					msg.getAddNum(),msg.getIncreNum()});
			st.addBatch(insert);
			
			st.executeBatch();
			con.commit();
			
		}
		catch(Exception ex)
		{
			Logger.getRootLogger().error("ImportLog ERROR:*********" + insert + ex.getMessage(),ex);
		}
		finally
		{
			CommonDB.closeDBConnection(con, st, null);
		}
		
	}
}

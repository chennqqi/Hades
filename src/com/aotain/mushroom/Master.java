package com.aotain.mushroom;

import java.io.IOException;

import org.apache.log4j.Logger;



/**
 * ������MR SPARK ��Driver ��������һ������������� �����ڵ㷢�͹�������Ϣ
 * @author Administrator
 *
 */
public class Master {
	
	private static Master _instance;
	
	public static Master getInstance()
	{
		if(_instance == null)
			_instance = new Master();
		return _instance;
	}
	
	public Master()
	{
		
	}
	
	

	private Logger log = Logger.getRootLogger();
	
	public void StartMaster(int port)
	{
		MasterConsole console = new MasterConsole(port);
		try {
			console.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.error("Master �����쳣",e);
		}
	}
}

package com.aotain.common;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DBConfigInit {
   
    private static String ips;
    
	public DBConfigInit(String dbConfig) 
	{
		System.out.println("dbconfig:" + dbConfig);
	    
	    try {   
	    	Properties prop = new Properties();   
		    InputStream in = new FileInputStream(dbConfig);
	        prop.load(in);   
	       
	        
	        ips = prop.getProperty("ips").trim(); 
	        
	        //postfix = prop.getProperty("postfix").trim();  
	        
	        //System.out.println("######filter url postfix:" + postfix);
	        
	    } catch (IOException e) {   
	        e.printStackTrace();   
	    }   
    }
	
	public String getIPs()
	{
		return this.ips;
	}
}

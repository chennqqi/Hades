package com.aotain.project.ud3clear;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;

import com.aotain.common.CommonDB;

public class ClassConfigini {
	 private static Map<String, String> classInfo = new HashMap<String,String>();

	 private static class LazyHolder {    
	       private static final ClassConfigini INSTANCE = new ClassConfigini();  
	       static {
	    	   getIniClassConfig();
	       }
		    public static void getIniClassConfig(){
		    	
		    	
	    		System.out.println("class_info ini");
	    		Connection c=CommonDB.getConnection("org.apache.hive.jdbc.HiveDriver","jdbc:hive2://172.1.16.41:10000/default","bms","bms168");
	    		Statement s=null;
			    ResultSet rs=null;
	    		try {
					 s=c.createStatement();
				     rs=s.executeQuery("select host, class_name from aotain_dim.to_url_class");
				   
				    while(rs.next()){
				    	String domain=rs.getString("host");
				    	String classname=rs.getString("class_name");
				    	if(domain!=null&&!domain.isEmpty()){
				             classInfo.put(domain, classname);
	                       
				    	}
						
				    }
				  
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
			
	    	}finally{
	    		CommonDB.close(rs, s, c);
	    	}
	    	
	    }
	    }    
	    private ClassConfigini (){}    
	    public static final ClassConfigini getInstance() {    
	    	
	    	return LazyHolder.INSTANCE;    
	    }
	    public  Map<String,String>getClassConfig(){
	    	return classInfo;
	    };

	   /* public static void main(String[] args) {
	    	ClassConfigini.getInstance();
	    	System.out.println(classInfo.size());
		}*/

}

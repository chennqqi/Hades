package com.aotain.project.ud3clear;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Ud3Config implements Serializable{
	  /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String outpath;
	  private String bathconfigPath;
      private String hdfsoutpath="/user/project/preDC/ud3_data";
      String []iniConfigs=new String[10];
	  public String getOutpath() {
		return outpath;
	}
	public void setOutpath(String outpath) {
		this.outpath = outpath;
	}
	public String getBathconfigPath() {
		return bathconfigPath;
	}
	public void setBathconfigPath(String bathconfigPath) {
		this.bathconfigPath = bathconfigPath;
	}
	public String getHdfsoutpath() {
		return hdfsoutpath;
	}
	public void setHdfsoutpath(String hdfsoutpath) {
		this.hdfsoutpath = hdfsoutpath;
	}
	public ArrayList<String> getOsRegexs() {
		return osRegexs;
	}
	public void setOsRegexs(ArrayList<String> osRegexs) {
		this.osRegexs = osRegexs;
	}
	public ArrayList<String> getOsVersionRegexs() {
		return osVersionRegexs;
	}
	public void setOsVersionRegexs(ArrayList<String> osVersionRegexs) {
		this.osVersionRegexs = osVersionRegexs;
	}
	public ArrayList<String> getDeviceRegexs() {
		return deviceRegexs;
	}
	public void setDeviceRegexs(ArrayList<String> deviceRegexs) {
		this.deviceRegexs = deviceRegexs;
	}
	public ArrayList<String> getBrowserRegexs() {
		return browserRegexs;
	}
	public void setBrowserRegexs(ArrayList<String> browserRegexs) {
		this.browserRegexs = browserRegexs;
	}
	public ArrayList<String> getBrowserVersionRegexs() {
		return browserVersionRegexs;
	}
	public void setBrowserVersionRegexs(ArrayList<String> browserVersionRegexs) {
		this.browserVersionRegexs = browserVersionRegexs;
	}
	public String getKwc() {
		return kwc;
	}
	public void setKwc(String kwc) {
		this.kwc = kwc;
	}
	public Map<String, String> getClassInfo() {
		return classInfo;
	}
	public void setClassInfo(Map<String, String> classInfo) {
		this.classInfo = classInfo;
	}
	private ArrayList<String> osRegexs = new ArrayList<String>();
		
	   private  ArrayList<String> osVersionRegexs = new ArrayList<String>();

	   private ArrayList<String> deviceRegexs = new ArrayList<String>();

	   private ArrayList<String> browserRegexs = new ArrayList<String>();

	   private  ArrayList<String> browserVersionRegexs = new ArrayList<String>();
	   private String kwc;
	   private Map<String, String> classInfo = new HashMap<String,String>();
	  private Map<String,Integer>stuffConfig=new HashMap<String,Integer>();
	   public Map<String, Integer> getStuffConfig() {
		return stuffConfig;
	}
	public void setStuffConfig(Map<String, Integer> stuffConfig) {
		this.stuffConfig = stuffConfig;
	}
	public void setConfig(){
		   try{
			// getConfigfiles();   
		  
			    findSystemIni("/home/systemIni.txt");
			    String configpath=iniConfigs[9];
			    getregexConfig( configpath+"/Regexs.txt");
				getkeyWordConfig(configpath+"/keyword");
				findStuff(configpath+"/suffix_config.cfg");
			}catch(Exception e){
				
			}
	   }
	private  void findSystemIni(String filePath){
		System.out.println(filePath);
		  
		File file=new File(filePath);
		try {
			 InputStreamReader read = new InputStreamReader(new FileInputStream(file));
             BufferedReader bufferedReader = new BufferedReader(read);
             String lineTxt = null;
             int i=0;
             while((lineTxt = bufferedReader.readLine()) != null){
				String stuff=lineTxt;
				iniConfigs[i]=stuff;
				System.out.println(stuff);
				i++;
				//stuffConfig.put(stuff, 1);
             }
        // System.err.println("stuffix has get--------------"+stuffConfig.size());
		} catch (Exception e) {
			e.printStackTrace();
		}
		  
}
		private void findStuff(String filePath){
		
			  
				File file=new File(filePath);
				try {
					 InputStreamReader read = new InputStreamReader(new FileInputStream(file));
	                 BufferedReader bufferedReader = new BufferedReader(read);
	                 String lineTxt = null;
	                 while((lineTxt = bufferedReader.readLine()) != null){
	    				String stuff=lineTxt.split("\\|")[0];
	    				stuffConfig.put(stuff, 1);
	                 }
	             System.err.println("stuffix has get--------------"+stuffConfig.size());
				} catch (Exception e) {
					e.printStackTrace();
				}
				  
		}
	   private void getConfigfiles(){
			 String cmd="hadoop fs -get "+bathconfigPath+"/regex/Regexs.txt"+ " /home";
	  	     String[] cmds = { "/bin/bash", "-c", cmd };
	  	     File f=new File("/home/Regexs.txt");
	  	     if(f.exists()){
	  	    	 f.delete();
	  	     }
	  	     f=new File("/home/keyword");
		     if(f.exists()){
		    	 f.delete();
		     }
		   /*  f=new File("/home/to_url_class.csv");
	  	     if(f.exists()){
	  	    	 f.delete();
	  	     }*/
	          Runtime run = Runtime.getRuntime();
	  	      Process p = null;
	  	 
	  	
	  	      try {
					p = run.exec(cmds);
					p.waitFor();
					 System.err.println("Regexs has get---------------------");
					cmd="hadoop fs -get "+bathconfigPath+"/keyword/keyword"+ " /home";
			  	    cmds[2]=cmd;
			        p=run.exec(cmds);
			        p.waitFor();
			        System.err.println("keyword has get-------------------");
			       /* cmd="hadoop fs -get "+bathconfigPath+"/urlclass/to_url_class.csv"+ " /home";
			  	    cmds[2]=cmd;
			        p=run.exec(cmds);
			        p.waitFor();
			        System.err.println("to_url_class.csv has get");*/
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	   private  void getregexConfig(
				 String fileName) throws IOException{
			
			 InputStream in=null;
			  InputStreamReader read=null;
			  BufferedReader  fis=null;
		      try {
		    
		    	in=  new BufferedInputStream(new FileInputStream(
		    			fileName));
					read = new InputStreamReader(
							  in,"UTF-8");
		    	fis = new BufferedReader(read);
//		    	fis = new BufferedReader(new FileReader(fileName));
		        String pattern = null;
		      //  System.out.println(fileName);
		        while ((pattern = fis.readLine()) != null) {
		        	
		          int flag=Integer.parseInt(pattern.split("#")[0]);
		          String re=pattern.split("#")[1].trim();
		          if(flag==1)
		        	  osRegexs.add(re);
		          else if(flag==2)
		        	  osVersionRegexs.add(re);
		          else if(flag==3)
		        	  deviceRegexs.add(re);
		          else if(flag==4)
		        	  browserRegexs.add(re);
		          else
		        	  browserVersionRegexs.add(re);
		        }
		        System.out.println(osVersionRegexs.size()+deviceRegexs.size()+browserRegexs.size()+browserVersionRegexs.size());
		       /// System.out.println("wanbi");
		      } catch (IOException ioe) {
		        System.err.println("Caught exception while parsing the  file '");
		      }finally{
		    	  if(in != null){
		    		  in.close();
		    	  }
		    	  if(fis != null){
		    		  fis.close();
		    	  }
		      }
			 
		 }
		 private  void  getkeyWordConfig(String fileName) throws Exception {
				
			 InputStream in=null;
			  InputStreamReader read=null;
			  BufferedReader  fis=null;
		      try {
		    		in=  new BufferedInputStream(new FileInputStream(
			    			fileName));
						read = new InputStreamReader(
								  in,"UTF-8");
			    	fis = new BufferedReader(read);
		        String pattern = null;
	      
		        while ((pattern = fis.readLine()) != null) {
		        	kwc=pattern+"#"+kwc;
		        
		        }
		        kwc=kwc.substring(0,kwc.length()-1);
		      } catch (IOException ioe) {
		        System.err.println("Caught exception while parsing the  file '");
		      }finally{
		    	  if(in != null){
		    		  in.close();
		    	  }
		    	  if(fis != null){
		    		  fis.close();
		    	  }
		      }
		   
		 }
		
}

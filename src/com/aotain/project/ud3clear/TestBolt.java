package com.aotain.project.ud3clear;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;

import com.aotain.project.ud3clear.ParseUA;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;















import com.aotain.common.CommonFunction;

























import com.hadoop.compression.lzo.LzopCodec;



public class TestBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	OutputCollector collector = null;

	public TestBolt(){
		
	}
public TestBolt(Ud3Config uc){
	
		this.outpath=uc.getOutpath();
		this.hdfsoutpath=uc.getHdfsoutpath();
		this.kwc=uc.getKwc();
		this.classInfo=uc.getClassInfo();
		this.osRegexs=uc.getOsRegexs();
		this.osVersionRegexs=uc.getOsVersionRegexs();
		this.browserRegexs=uc.getBrowserRegexs();
		this.browserVersionRegexs=uc.getBrowserVersionRegexs();
		this.deviceRegexs=uc.getDeviceRegexs();
	}
public TestBolt(ArrayList<String> browserRegexs){
	this.browserRegexs= browserRegexs;
	
}
	public String getOutpath() {
	return outpath;
}
public void setOutpath(String outpath) {
	this.outpath = outpath;
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
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}
  private String outpath;
  private String areaid="44300";
  private String ip;
  private String hdfsoutpath="/user/project/preDC/ud3_data";
  private  ArrayList<String> osRegexs = new ArrayList<String>();
	
   private  ArrayList<String> osVersionRegexs = new ArrayList<String>();

   private ArrayList<String> deviceRegexs = new ArrayList<String>();

   private ArrayList<String> browserRegexs = new ArrayList<String>();

   private  ArrayList<String> browserVersionRegexs = new ArrayList<String>();
   private String kwc;
   private Map<String, String> classInfo = new HashMap<String,String>();
   List<String>datas=new ArrayList<String>();
   int fileoutCount=0;
   long filetime=0;
   private String timepath="20151226";
   private String configpath="";
   Map<String,List<String>>mdata=new HashMap<String,List<String>>() ;
   private Map<String,Integer>stuffConfig=new HashMap<String,Integer>();
	@Override
	public void execute(Tuple tuple) {
	
		try {
			
			
				
				pasedata( tuple) ;
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.err.println("http data exception");
			//System.out.println(tuple.getLong(0));
			Logger.getRootLogger().error(e.getMessage(),e);
			//System.out.println("http error data:"+tuple.getString(0));
			//.getRootLogger().error("windowcalbolt exception====",e);
			collector.fail(tuple);
			//throw new FailedException(e.getMessage());
		}	

		
		 
	}
	private void putDataToHdfs(){
		  String filename=ip+"_"+filetime;
			if(fileoutCount==1000){
		  Set<Entry<String, List<String>>> set = mdata.entrySet();
			Iterator<Entry<String, List<String>>> it = set.iterator();
		   
			while(it.hasNext()){
		    	Entry<String, List<String>>en =it.next();
		    	String mtimepath=en.getKey();
		    	List<String>outdata=en.getValue();
			         if(outdata.size()>0){
			    	 try {
			    		
			    		 outData(mtimepath, filename, hdfsoutpath, mtimepath, outdata);
							outdata.clear();
							mdata.put(mtimepath, outdata);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
					
				 }
			         }         
				    	
				    	
			     }
			   fileoutCount=0;
		    }
		 

	}

		
	
	private void outData(String  outpath, String  filename, String hdfsoutpath,
		     String timepath,List<String> datas){
		 try {
			 long filetime=0;
			 int radom=RandomUtils.nextInt(1000);
			 long time=System.currentTimeMillis();
			 SimpleDateFormat dfTime = new SimpleDateFormat("yyyyMMddHHmmss");
			 String stime=dfTime.format(new Date(time));
				 filetime=time+radom;
				 String ip=InetAddress.getLocalHost().getHostAddress(); 
				 filename=ip+"_"+stime+"_"+filetime;

	

				  Configuration conf = new Configuration();
				  conf.addResource(new Path("/etc/hadoop/conf/core-site.xml")); 
			      conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml")); 
			      conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml")); 
		
				//  Logger.getLogger(CUDataImportBolt.class).info("###HDFS :" + conf.get("fs.defaultFS"));
					conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
				  String path = "hdfs://nameservice1"+hdfsoutpath+"/"+timepath ;
				  FileSystem hadoopFS = FileSystem.get(conf);
		            Path hadPath=new Path(path);
		      File file=new File(filename);
		            
				    if(!hadoopFS.exists(hadPath))
				    {
				    	boolean lb = hadoopFS.mkdirs(hadPath);
				    	Logger.getLogger(Taskimpl.class).info("###Create path:" + path + "  " + String.valueOf(lb));
				    }
				    FSDataOutputStream fsOut = hadoopFS.create(new Path(path+"/"+file.getName())); 
                     StructObjectInspector inspector = 
							(StructObjectInspector) ObjectInspectorFactory
							.getReflectionObjectInspector(MyRow.class,
									ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
					OrcSerde serde = new OrcSerde();
					OutputFormat outFormat = new OrcOutputFormat();
					JobConf conff = new JobConf();
					conff.addResource(conf);
					RecordWriter writer = outFormat.getRecordWriter(hadoopFS, conff,
							hadPath.toString(), Reporter.NULL);
					writer.write(NullWritable.get(),
							serde.serialize(new MyRow("075518546@168.com",20), inspector));
					writer.write(NullWritable.get(),
							serde.serialize(new MyRow("075514550@168.com",22), inspector));
					writer.write(NullWritable.get(),
							serde.serialize(new MyRow("075547889@168.com",30), inspector));
					writer.close(Reporter.NULL);
					
					hadoopFS.close();
					System.out.println("write success .");
		  /* LzopCodec lzo = new LzopCodec();
		            lzo.setConf(conf);
		           
		          CompressionOutputStream lzoOut = lzo.createOutputStream(fsOut);
		        String line=null;
		    	StringBuilder sb = new StringBuilder();
		   		
			     for(int i=0;i<datas.size();i++){
			    	 line=datas.get(i);
			    	 line += "\n";  
				   	 sb.append(line);
				   	line=null;
			     }
				  ByteArrayInputStream inputStream 
	            	= new ByteArrayInputStream(sb.toString().getBytes("UTF-8"));
		    	  String line2 = null;
		    	  BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
			        while ((line2 = br.readLine()) != null) { 
			        	line2 = line2 + "\n";
			            byte[] bytes = line2.getBytes();  
			            lzoOut.write(bytes, 0, bytes.length);  
			        }
			     lzoOut.close(); 
			     fsOut.close();*/
			 	Logger.getLogger(Taskimpl.class).info("###Create path:" + path + "has put data  " );
			     }catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					};
	}
	static class MyRow implements Writable {
		String username;
		int age;
		
		MyRow(String username,int age){
			this.username = username;
			this.age = age;
		}
		@Override
		public void readFields(DataInput arg0) throws IOException {
			throw new UnsupportedOperationException("no write");
		}
		@Override
		public void write(DataOutput arg0) throws IOException {
			throw new UnsupportedOperationException("no read");
		}
		
	}
	private void pasedata(Tuple input){
		String row=input.getString(0);
		
		String items[]  = row.split("\\|", -1);
		//String result = "";
		/*userAccount                  string                                      
	PROTOCOLTYPE               string                                      
		SOURCEIP                  string                                      
		DESTINATIONIP                 string                                      
		SOURCEPORT                     string                                      
		DESTINATIONPORT                   string                                      
		DOMAINNAME                 string                                      
		URL              string                                      
		REFERER                 string                                      
		USERAGERN              string                                      
		COOKIE                  string                                      
		accesstime              bigint                                      
	*/
	    
		
		if(items.length != 12){
			System.err.println("the data length has error");
			 return ;
		}
			
		
		 //验证用户账号的合法�?
		if(!validateUser(items[0])){
			System.err.println("the user has error");
			 return ;
		}
		
		 String accesstime = CommonFunction.findByRegex(items[11].trim(), "[0-9]*", 0);
		
		 if(accesstime==null){
				System.err.println("the time has error");
				 return ;
			}
		
		 if(items[11].trim().isEmpty()){
				System.err.println("the time has error");
				 return ;
			}

         StringBuilder sb = new StringBuilder();
         String httpcloums[]=new String[21];
         httpcloums[0]=areaid;
         httpcloums[1]=items[0];
         httpcloums[2]=items[2];
        
         String url=CommonFunction.decodeBASE64(items[7]);
         if(url!=null){
         httpcloums[4]=url.trim();
         String stuffix=url.substring(url.lastIndexOf(".")+1);
         if(stuffConfig.get(stuffix)!=null)
        	 return;
         }
         String domain=CommonFunction.getDomain(url);
         httpcloums[3]=domain;
         String refer=CommonFunction.decodeBASE64(items[8]);
         if(refer!=null)
         httpcloums[5]=refer.trim();
         String preua=CommonFunction.decodeBASE64(items[9]);
        
        final ParseUA pu=new ParseUA(osRegexs, osVersionRegexs, deviceRegexs, browserRegexs, browserVersionRegexs);
      
         String praseUAS[]=pu.getUAinfo(preua).split(",");
         for(int i=0;i<praseUAS.length;i++){
        	 if("null".equals(praseUAS[i]))
            	 praseUAS[i]=""; 
        	 if(praseUAS[i]!=null&&i!=1)
        	 praseUAS[i]=praseUAS[i].toLowerCase();
         }
        
         httpcloums[6]=praseUAS[0];
         httpcloums[7]=praseUAS[1];
         httpcloums[8]=praseUAS[3];
         httpcloums[9]=praseUAS[4];
         httpcloums[10]=praseUAS[2];
         if( httpcloums[6]!=null){
        	 String opers= httpcloums[6].toLowerCase();
        	 if(opers.equals("iphone")||opers.equals("ipad")||opers.equals("os"))
        		 httpcloums[6]="ios";
         }
  
         httpcloums[11]=items[11];
         httpcloums[12]=CommonFunction.decodeBASE64(items[10]);
         
         httpcloums[13]=CommonFunction.parseKeyWord(url,kwc,domain);
         if(httpcloums[13].length()==0&&httpcloums[5].contains("www.baidu.com")){
        	 httpcloums[13]=CommonFunction.parseKeyWord(url,kwc,domain);
         }
        	 
       if(classInfo.get(domain)!=null)
            httpcloums[14]=classInfo.get(domain);
         else
        	 httpcloums[14]="0";
         httpcloums[15]=CommonFunction.getDomain(refer);
         if(classInfo.get( httpcloums[15])!=null)
             httpcloums[16]=classInfo.get(httpcloums[15]);
          else
         	 httpcloums[16]="0";
         httpcloums[17]=preua;
         httpcloums[18]=items[3];
         httpcloums[19]=items[4];
         httpcloums[20]=items[5];
         SimpleDateFormat dfTime = new SimpleDateFormat("yyyyMMddHHmmss");
       
		 for(int i = 0;i< 21;i++)
		 {
			 
			 String col = httpcloums[i];
			/* if(i==1 && !httpcloums[2].trim().isEmpty() 
					 && AdslMap.containsKey(httpcloums[2].trim()))
			 {
				 String username = AdslMap.get(httpcloums[2].trim());
				 col = username;
			 }*/
			 
			 if(i==11)
			 {
				 Date dAccessTime = new Date(Long.parseLong(items[11].trim())*1000L);
				 String strAccessTime = dfTime.format(dAccessTime);
				 col = strAccessTime;
			 }
			 if("".equals(col)){
				 col=" ";
			 }
			 if(col.contains("|")){
				 col=col.replace("|", "#");
			 }
				
	         sb.append(col + "|");	 
		 }
		 row = sb.toString().substring(0, sb.length() - 1 );
		 row=row.replace("\n", "");
		 row=row.replace("\r", "");
		
		 SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
		  
		 Date dStartTime = new Date(Long.parseLong(items[11].trim())*1000L);
		 String strDate = df.format(dStartTime);
		
		  long sysTime=System.currentTimeMillis();
		  Date sysDate=new Date(sysTime);
		  int hour=sysDate.getHours();
		  if(hour>=1){
			  strDate= df.format(sysDate);
		  }
		  List<String>dlist=mdata.get(strDate);
			    if(dlist!=null){
		             dlist.add(row);
					 mdata.put(strDate, dlist);
				 }else{
					 dlist=new ArrayList<String>();
					 dlist.add(row);
					 mdata.put(strDate, dlist);
				 }
		    fileoutCount++;
			putDataToHdfs();  	
	/*     if(flag){
	    	 
	    	
	    	    timepath=strDate;
		    	
	    	    List<String>outdata=new ArrayList<String>();
	    		  outdata.addAll(datas);
	    			datas.clear();
	    		   String filename="";
				    Task ta=new Taskimpl(  outpath,  filename, hdfsoutpath ,timepath,outdata);
		    		   ThreadPool tp=ThreadPool.getInstance();
		    		   tp.addTask(ta);
					
					outdata.clear();

	    	  
	    	  
	      }*/
	     
			collector.ack(input);	
	}

	public String getAreaid() {
		return areaid;
	}
	public void setAreaid(String areaid) {
		this.areaid = areaid;
	}
	public Map<String, Integer> getStuffConfig() {
		return stuffConfig;
	}
	public void setStuffConfig(Map<String, Integer> stuffConfig) {
		this.stuffConfig = stuffConfig;
	}
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		 try {
			
			ip=InetAddress.getLocalHost().getHostAddress(); 
			getClassConfig(configpath+"/to_url_class.csv") ;
			System.out.println(osRegexs.size()+"urlclass:"+classInfo.size());
			System.err.println("my_config has get");
			System.out.println("my stuff config"+stuffConfig.size());
		 } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.collector = collector ;
	}
	 private  void getClassConfig(String fileName) throws Exception {
		/* ClassConfigini c=ClassConfigini.getInstance();
		 classInfo=c.getClassConfig();
		 System.out.println("my classinfo"+classInfo.size());*/
		 InputStream in=null;
		  InputStreamReader read=null;
		  BufferedReader  fis=null;
	      try {
	    	  in=  new BufferedInputStream(new FileInputStream(
		    			fileName));
					read = new InputStreamReader(
							  in,"UTF-8");
		    	fis = new BufferedReader(read);
//	    	fis = new BufferedReader(new FileReader(fileName));
	        String pattern = null;
	        String[]cols;
	        while ((pattern = fis.readLine()) != null) {
	        	 cols = pattern.split(",");
	        	 if(cols.length>3)
	        		 classInfo.put(cols[4], cols[3]);
	        
	        }
	      
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
	
	 private  boolean validateUser(String username)
		{
			boolean ret = false;
			
			String aa = CommonFunction.findByRegex(username, "^([a-zA-Z0-9_\\-\\.]{1,50})@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.)|(([a-zA-Z0-9\\-]+\\.)+))(com|cn|net|com.cn|org|gd)(\\]?)$", 0);
			if(aa == null)
			{
				aa = CommonFunction.findByRegex(username, "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}", 0);
				if(aa == null)
					ret = false;
				else
					ret = true;
			}
			else
			{
				ret = true;
			}
			
			return ret;
		}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		/*Map<String,Object> conf = new HashMap<String,Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,10);
*/
		return null;
	}
	public String getConfigpath() {
		return configpath;
	}
	public void setConfigpath(String configpath) {
		this.configpath = configpath;
	}



}

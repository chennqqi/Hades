package com.aotain.ods;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.aotain.common.CommonFunction;
import com.aotain.ods.ua.Browser;
import com.aotain.ods.ua.OperatingSystem;
import com.aotain.ods.ua.ParseUA;
import com.aotain.ods.ua.UserAgent;
import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.mapreduce.LzoTextInputFormat;

public class PreHTTPDCSpark {
    
   
	  private static String[] cols;
	public static void main(String[] args) {
		   //System.out.println("0###OK################################");
		
		if (args.length !=8){
            System.err.printf("Usage: %s <input><output><config><remark>");
            System.exit(1);
		}          
		
		//HTTPDCSpark spark = new HTTPDCSpark();
		HTTPDCFunc(args);
		   //System.out.println("1###OK################################");
	}
	
	 public static int HTTPDCFunc(final String[] args){
		 try
		 {
		    SparkConf sparkConf = new SparkConf().setAppName("PRE HTTP DC Spark");
		    Configuration config = new Configuration();
		    config.addResource("/etc/hadoop/conf");
		    System.out.println("#%%%HDFS:" + config.get("fs.defaultFS"));
		    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		 
		    
		    String sourcePath = args[0];
		    System.out.println("sourcePath: " + sourcePath);
		    //FileSystem fsSource = FileSystem.get(URI.create(sourcePath),config);
		    //Path pathSource = new Path(sourcePath);
		    //if(!fsSource.exists(pathSource))
		   // {
		    //	return 0;
		    //}
		    
		    //Ŀ��Ŀ¼ά��
		    String targetPath = args[1];
		    System.out.println("targetPath: " + targetPath);
		    FileSystem fsTarget = FileSystem.get(URI.create(targetPath),config);
		    Path pathTarget = new Path(targetPath);
		    if(fsTarget.exists(pathTarget))
		    {
		    	fsTarget.delete(pathTarget, true);
		    	System.out.println("Delete path " + targetPath);
		    }
		    
		    //ר���˺�����
		    String confuri = args[2];//"/user/hive/warehouse/aotain_dim.db/to_ref_terminalinfo/";
		    String keywordConfig="";
		    keywordConfig= getkeyWordConfig(confuri,config,keywordConfig);
            final String kwc=keywordConfig;
            confuri=args[4];
            final Map<String, String> classInfo = new HashMap<String,String>();
          
            getClassConfig(confuri,config,classInfo);
            final ArrayList<String> osRegexs = new ArrayList<String>();
        	
            final ArrayList<String> osVersionRegexs = new ArrayList<String>();
        	
            final ArrayList<String> deviceRegexs = new ArrayList<String>();
        	
            final ArrayList<String> browserRegexs = new ArrayList<String>();
        	
            final ArrayList<String> browserVersionRegexs = new ArrayList<String>();
        	  confuri=args[6];
        	 getregexConfig(osRegexs, osVersionRegexs, deviceRegexs, browserRegexs, browserVersionRegexs, confuri, config);
        	
        	 confuri = args[7];//"/user/hive/warehouse/aotain_dim.db/to_ref_terminalinfo/";
 		     FileSystem fs = FileSystem.get(URI.create(confuri),config); 
             FSDataInputStream in = null; 
             final HashMap<String,String> AdslMap = new HashMap<String,String> ();
            
             	//IPCYW9971632,218.17.90.26,218.17.90.62,255.255.255.192,
             Path path = new Path(confuri);
             if(fs.exists(path))
             {
             	System.out.println(confuri); 
             	System.out.println("exist conf file !!!!!!"); 
             	try{
             		if(fs.isDirectory(path))
             		{            		
             			for(FileStatus file:fs.listStatus(path))
             			{
 			           		System.out.println(confuri); 
 			           		System.out.println("exist conf file !!!!!!"); 
 			           		try{
 			           			//file.readFields(in);
 			           			in = fs.open(file.getPath());
 			    	           	BufferedReader bis = new BufferedReader(new InputStreamReader(in,"UTF8")); 
 			    	           	String line = "";
 			    	           	while ((line = bis.readLine()) != null) {  
 			    	           		String items[] = line.split(",",-1);
 			    	           		AdslMap.put(items[1], items[0]);//IP,ADSL
 			    	           	}
 			           			//deviceList.deleteCharAt(deviceList.length()-1);//ȥ�����һ������
 			           			//System.out.println(deviceList.toString()); 
 				            
 			           		}finally{
 			           	  IOUtils.closeStream(in);
 			           		}
             			}
             		}
             	}finally{ 
                     IOUtils.closeStream(in); 
             	}
             }
             	else
             	{
             		System.out.println(confuri); 
             		System.out.println("not exist file !!!!!!"); 
             	}
             
 		    
     
          
		    //JavaRDD<String> lines = ctx.textFile(ip, 1);
		    JavaPairRDD<LongWritable,Text> tt=ctx.newAPIHadoopFile(sourcePath,LzoTextInputFormat.class,  LongWritable.class, Text.class,config);
		    
		   // tt.mapToPair(f)
		    JavaPairRDD<String, String> s = tt.mapToPair(new PairFunction<Tuple2<LongWritable,Text>, String,String>() {
		    	
		    	/**
				 * 
				 */
				private static final long serialVersionUID = 1L;
				String strCurDate = args[3]; //����������
				@Override
				public Tuple2<String, String> call(Tuple2<LongWritable, Text> v1) throws Exception {
					
					String row = v1._2.toString();
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
					Tuple2<String, String> result = new Tuple2<String, String>("", "");
					if(items.length != 12){
						result=new Tuple2<String, String>("errodata"+"|"+row, "");
						 return result;
						 }

					 //��֤�û��˺ŵĺϷ���
					if(!validateUser(items[0]))
					{
						result=new Tuple2<String, String>("errodata"+"|"+row, "");
						 return result;
						 }
					 
					  String accesstime = CommonFunction.findByRegex(items[11], "[0-9]*", 0);
					 if(accesstime==null)
					 {
						 result=new Tuple2<String, String>("errodata"+"|"+row, "");
							 return result;
							 }
					 
					 if(items[11].trim().isEmpty())
					 {
						 result=new Tuple2<String, String>("errodata"+"|"+row, "");
							 return result;
							 }
					 
					 
					 
					 SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
					 Date dStartTime = new Date(Long.parseLong(items[11])*1000L);
					 //String strDate = df.format(dStartTime);
					 
					 
					 try {
						Date curDate = df.parse(strCurDate);
						Date pre2Date = new Date(curDate.getTime() - 24*3600*1000L);
						Date nextDate = new Date(curDate.getTime() + 24*3600*1000L);
						
						if(dStartTime.getTime() < pre2Date.getTime() || dStartTime.getTime() >= nextDate.getTime())
						{
							result=new Tuple2<String, String>("errodata"+"|"+row, "");
							 return result;
							 }
						
					 } catch (ParseException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
						
					 }
                     StringBuilder sb = new StringBuilder();
                     String httpcloums[]=new String[21];
                     httpcloums[0]=args[5];
                     httpcloums[1]=items[0];
                     httpcloums[2]=items[2];
                    
                     String url=CommonFunction.decodeBASE64(items[7]);
                     httpcloums[4]=url;
                     String domain=CommonFunction.getDomain(url);
                     httpcloums[3]=domain;
                     String refer=CommonFunction.decodeBASE64(items[8]);
                     httpcloums[5]=refer;
                     
                     String preua=CommonFunction.decodeBASE64(items[9]);
                     final ParseUA pu=new ParseUA(osRegexs, osVersionRegexs, deviceRegexs, browserRegexs, browserVersionRegexs);
                     String praseUAS[]=pu.getUAinfo(preua).split(",");
                     httpcloums[6]=praseUAS[0];
                     httpcloums[7]=praseUAS[1];
                     httpcloums[8]=praseUAS[3];
                     httpcloums[9]=praseUAS[4];
                     httpcloums[10]=praseUAS[2];
                     if( httpcloums[6]!=null){
                    	 String opers= httpcloums[6].toLowerCase();
                    	 if(opers.equals("iphone")||opers.equals("ipad"))
                    		 httpcloums[6]="IOS";
                     }
                   /*  UserAgent ua=UserAgent.parseUserAgentString(preua);
                     if(ua!=null){
                     OperatingSystem oper=ua.getOperatingSystem();
                     Browser bw=ua.getBrowser();
                     String opersys=oper.getName();
                     String opersyser=oper.getOsVersion();
                     String browse=bw.getName();
                     if(ua.getBrowserVersion()!=null){
                     String browserver=ua.getBrowserVersion().getVersion();
                     httpcloums[9]=browserver;
                     }else{
                     httpcloums[9]=" ";
                     }
                     httpcloums[6]=opersys;
                     httpcloums[7]=opersyser;
                     httpcloums[8]=browse;
                   
                     }else{
                    	 httpcloums[6]=" ";
                         httpcloums[7]=" ";
                         httpcloums[8]=" ";
                         httpcloums[9]=" ";
                     }
                     httpcloums[10]="yyyy";*/
                     httpcloums[11]=items[11];
                     httpcloums[12]=CommonFunction.decodeBASE64(items[10]);
                     
                     httpcloums[13]="";//CommonFunction.parseKeyWord(url, kwc);
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
						 if(i==1 && !httpcloums[2].trim().isEmpty() 
								 && AdslMap.containsKey(httpcloums[2].trim()))
						 {
							 String username = AdslMap.get(httpcloums[2].trim());
							 col = username;
						 }
						 
						 if(i==11)
						 {
							 Date dAccessTime = new Date(Long.parseLong(items[11])*1000L);
							 String strAccessTime = dfTime.format(dAccessTime);
							 col = strAccessTime;
						 }
						 if("".equals(col))
							 col=" ";
				         sb.append(col + "|");	 
					 }
					 row = sb.toString().substring(0, sb.length() - 1 );
					
					 result = new Tuple2<String, String>(row, "");
					 return result;
				}
			}).filter(new Function<Tuple2<String, String>,Boolean>(){

				@Override
				public Boolean call(Tuple2<String, String> v1) throws Exception {
					// TODO Auto-generated method stub
					if(v1._1.isEmpty())
						return false;
					else
						return true;
				}
		    	
		    }).repartition(512);
			
		    s.saveAsHadoopFile(targetPath, Text.class, Text.class, TextOutputFormat.class, LzopCodec.class);
		    
		    ctx.stop();
		    return 0;
		 }
		 catch(Exception ex)
		 {
			 ex.printStackTrace();
			 return 1;
		 }
		 
		   
	   }
	 private static void getregexConfig(ArrayList<String> osRegexs,ArrayList<String> osVersionRegexs ,
			 ArrayList<String> deviceRegexs,ArrayList<String> browserRegexs , ArrayList<String> browserVersionRegexs,
			 String fileName, Configuration conf) throws IOException{
		
		 FileSystem fs = FileSystem.get(URI.create(fileName),conf); 
		  FSDataInputStream in = null; 
		  BufferedReader  fis=null;
	      try {
	    	Path mobilePath = new Path(fileName);
	    	in = fs.open(mobilePath);
	    	fis = new BufferedReader(new InputStreamReader(in,"UTF-8"));
//	    	fis = new BufferedReader(new FileReader(fileName));
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
	 private static String  getkeyWordConfig(String fileName,  Configuration conf,String keywordConfig) throws Exception {
		
		  FileSystem fs = FileSystem.get(URI.create(fileName),conf); 
		  FSDataInputStream in = null; 
		  BufferedReader  fis=null;
	      try {
	    	Path mobilePath = new Path(fileName);
	    	in = fs.open(mobilePath);
	    	fis = new BufferedReader(new InputStreamReader(in,"UTF-8"));
//	    	fis = new BufferedReader(new FileReader(fileName));
	        String pattern = null;
       
	        while ((pattern = fis.readLine()) != null) {
	        	keywordConfig=pattern+"#"+keywordConfig;
	        
	        }
	        keywordConfig=keywordConfig.substring(0,keywordConfig.length()-1);
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
	      return keywordConfig;
	 }
	 private static void getClassConfig(String fileName,Configuration conf,Map<String,String> classMap) throws Exception {
			
		 FileSystem fs = FileSystem.get(URI.create(fileName),conf); 
		  FSDataInputStream in = null; 
		  BufferedReader  fis=null;
	      try {
	    	Path mobilePath = new Path(fileName);
	    	in = fs.open(mobilePath);
	    	fis = new BufferedReader(new InputStreamReader(in,"UTF-8"));
//	    	fis = new BufferedReader(new FileReader(fileName));
	        String pattern = null;
	        String[]cols;
	        while ((pattern = fis.readLine()) != null) {
	        	 cols = pattern.split(",");
	        	 if(cols.length>3)
	        		 classMap.put(cols[4], cols[3]);
	        
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
	 private static boolean validateUser(String username)
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
	
}

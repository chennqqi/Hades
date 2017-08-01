package com.aotain.ods;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.aotain.common.CommonFunction;
import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.mapreduce.LzoTextInputFormat;

public class HTTPDCSpark {

	public static void main(String[] args) {
		   //System.out.println("0###OK################################");
		
		if (args.length != 4){
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
		    SparkConf sparkConf = new SparkConf().setAppName("HTTP DC Spark");
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
		    
		    //目标目录维护
		    String targetPath = args[1];
		    System.out.println("targetPath: " + targetPath);
		    FileSystem fsTarget = FileSystem.get(URI.create(targetPath),config);
		    Path pathTarget = new Path(targetPath);
		    if(fsTarget.exists(pathTarget))
		    {
		    	fsTarget.delete(pathTarget, true);
		    	System.out.println("Delete path " + targetPath);
		    }
		    
		    //专线账号配置
		    String confuri = args[2];//"/user/hive/warehouse/aotain_dim.db/to_ref_terminalinfo/";
		    FileSystem fs = FileSystem.get(URI.create(confuri),config); 
            FSDataInputStream in = null; 
            final HashMap<String,String> AdslMap = new HashMap();
           
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
			           			//deviceList.deleteCharAt(deviceList.length()-1);//去掉最后一个逗号
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
				String strCurDate = args[3]; //参数配置来
				@Override
				public Tuple2<String, String> call(Tuple2<LongWritable, Text> v1) throws Exception {
					
					String row = v1._2.toString();
					String items[]  = row.split("\\|", -1);
					//String result = "";
					/*areaid                  string                                      
					username                string                                      
					srcip                   string                                      
					domain                  string                                      
					url                     string                                      
					refer                   string                                      
					opersys                 string                                      
					opersysver              string                                      
					browser                 string                                      
					browserver              string                                      
					device                  string                                      
					accesstime              bigint                                      
					cookie                  string                                      
					keyword                 string                                      
					urlclassid              int                                         
					referdomain             string                                      
					referclassid            int 
					ua                      String*/
					Tuple2<String, String> result = new Tuple2<String, String>("", "");
					if(items.length != 18)
						 return result;

					 //验证用户账号的合法性
					 if(!validateUser(items[1]))
						 return result;
					 
					 String accesstime = CommonFunction.findByRegex(items[11], "[0-9]*", 0);
					 if(accesstime==null)
						 return result;
					 
					 if(items[11].trim().isEmpty())
						 return result;
					 
					 
					 
					 SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
					 Date dStartTime = new Date(Long.parseLong(items[11])*1000L);
					 //String strDate = df.format(dStartTime);
					 
					 
					 try {
						Date curDate = df.parse(strCurDate);
						Date pre2Date = new Date(curDate.getTime() - 24*3600*1000L);
						Date nextDate = new Date(curDate.getTime() + 24*3600*1000L);
						
						if(dStartTime.getTime() < pre2Date.getTime() || dStartTime.getTime() >= nextDate.getTime())
							return result;
						
					 } catch (ParseException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
						
					 }
					 String strKey = String.format(items[1]);
					 
					 SimpleDateFormat dfTime = new SimpleDateFormat("yyyyMMddHHmmss");
					 
					 StringBuilder sb = new StringBuilder();
					 for(int i = 0;i< items.length;i++)
					 {
						 String col = items[i];
						 if(i==1 && !items[2].trim().isEmpty() 
								 && AdslMap.containsKey(items[2].trim()))
						 {
							 String username = AdslMap.get(items[2].trim());
							 col = username;
						 }
						 
						 if(i==11)
						 {
							 Date dAccessTime = new Date(Long.parseLong(items[11])*1000L);
							 String strAccessTime = dfTime.format(dAccessTime);
							 col = strAccessTime;
						 }
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

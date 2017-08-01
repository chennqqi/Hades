package com.aotain.project.terminal;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
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
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.aotain.common.CommonFunction;
import com.hadoop.mapreduce.LzoTextInputFormat;

/**
 * UA终端类型关联爬虫数据
 * @author Administrator
 *
 */
public class TerminalDataJoin {

	public static void main(String[] args) {
		   //System.out.println("0###OK################################");
		
		 if (args.length != 2){

          System.err.printf("Usage: %s <input><output>");
          System.exit(1);   

		}                     
		
		//HTTPDCSpark spark = new HTTPDCSpark();
		int ret = TerminalSparkFunc(args);
		System.exit(ret);
		   //TerminalSparkFunc.out.println("1###OK################################");
	}
	
public static int TerminalSparkFunc(final String[] args){
		
		try
		{
			System.setProperty("spark.shuffle.consolidateFiles","true");
			SparkConf sparkConf = new SparkConf().setAppName("Terminal DataJoin Spark");
			sparkConf.set("spark.shuffle.consolidateFiles", "true");
		    Configuration config = new Configuration();
		    config.addResource("/etc/hadoop/conf");
		    System.out.println("#%%%HDFS:" + config.get("fs.defaultFS"));
		    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		    
		    String sourcePath = args[0];
		    System.out.println("sourcePath: " + sourcePath);
		   	    
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
		    
		    
		    //读取爬虫得到的配置数据
		    String confuriPhone = "/user/hive/warehouse/aotain_dim.db/to_ref_phoneinfo/";
		    FileSystem fsPhone = FileSystem.get(URI.create(confuriPhone),config); 
            FSDataInputStream inPhone = null; 
            ArrayList<String> keylistPhone = new ArrayList<String>();
             
            Map<String,String> map=new HashMap<String,String>();
            try{
            	Path path = new Path(confuriPhone);
            	if(fsPhone.exists(path))
            	{
            		System.out.println(confuriPhone); 
            		System.out.println("exist conf file !!!!!!"); 
            		try{
            	  
            			if(fsPhone.isDirectory(path))
            			{            		
            				for(FileStatus file:fsPhone.listStatus(path))
            				{
            					//file.readFields(in);
            					inPhone = fsPhone.open(file.getPath());
    	            			BufferedReader bis = new BufferedReader(new InputStreamReader(inPhone,"UTF8")); 
    	            			String line = "";
    	            			while ((line = bis.readLine()) != null) {  
    	            				String[] arr = line.split(",",-1);
    	            				if(arr[1].contains("型号"))
    	            				{
    	            					//String str = "Phone" + ":" +arr[0]+":"+arr[2];
	    	            				//if(!keylistPhone.contains(str))
	    	            				{
	    	            					//keylistPhone.add(str);
	    	            					map.put("Phone" + ":" +arr[0], arr[2]);
	    	            				}
    	            				}
    	            			}
            				}
	            			
            			}
            			//deviceList.deleteCharAt(deviceList.length()-1);//去掉最后一个逗号
            			//System.out.println(deviceList.toString()); 
	             
            	  }finally{
            	  IOUtils.closeStream(inPhone);
            	  }
            	}
            	else
            	{
            		System.out.println(confuriPhone); 
            		System.out.println("not exist file !!!!!!"); 
            	}
            }finally{ 
                IOUtils.closeStream(inPhone); 
            }
		    
            final Map<String,String> mapConfig = map;
		    
		    //JavaRDD<String> lines = ctx.textFile(ip, 1);
		    JavaPairRDD<LongWritable,Text> tt=ctx.newAPIHadoopFile(sourcePath,TextInputFormat.class,  LongWritable.class, Text.class,config);
		   // tt.mapToPair(f)
		    JavaPairRDD<String, String> s = tt.mapToPair(new PairFunction<Tuple2<LongWritable,Text>, String,String>() {

		    	
		    	/**
				 * 
				 */
				private static final long serialVersionUID = 1L;
				@Override
				public Tuple2<String, String> call(Tuple2<LongWritable, Text> v1) throws Exception {
					
					Tuple2<String, String> result = new Tuple2<String, String>("", "");
					String strKey = "";
					//获取到需要过滤的url
					
					
					
					
					//行字段值
					String[] items = v1._2.toString().split(",",-1);
					/**新需求修改后字段结构 2014-12-24 turk
					  *0 UserName
					   1 devicecount
					   2 device
					   3 null
					  */
					 if(items.length != 4)
						 return result;
					 
					 String device = items[2];
					 String deviceName = "";
				     String fullName = "";
				     String DeviceType = "";
						float fMaxRate = 0;
						// TODO Auto-generated method stub
						for(String mapKey : mapConfig.keySet())
						{
							String[] arrDev = mapKey.split(":",-1);
							float v = CommonFunction.Similar(device,arrDev[1]);
							if(v > fMaxRate && v >= 0.5)
							{
								fMaxRate = v;
								deviceName = mapConfig.get(mapKey);
								fullName = arrDev[1];
								DeviceType = arrDev[0];
								break;
							}
						}
					//username string,device string,devicename string,fullname string,devicetype string,
						//devicecount int,remark string
					
				     String value = items[0] + "|" + device + "|" + deviceName + "|" + fullName + "|" + DeviceType + "|" + items[1] + "|";
					 
					 result = new Tuple2<String, String>(value, "");
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
		    }).distinct().repartition(512);
		    
		    
		    s.saveAsHadoopFile(targetPath, Text.class, Text.class, TextOutputFormat.class);
		    ctx.stop();
		    return 0;
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			return 1;
	 	}
	}
	
	
}

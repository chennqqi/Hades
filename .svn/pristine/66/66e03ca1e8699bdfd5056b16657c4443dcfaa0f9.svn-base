package com.aotain.ods;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

import com.aotain.common.CommonFunction;
import com.aotain.common.ObjectSerializer;

/**
 * 集团DPI MR
 * @author Administrator
 *
 */
public class DPIDriver extends Configured implements Tool{

	private static String dbdriver;   
    private static String dburl;
    private static String dbuser;
    private static String dbpassword;
    
    private static String postfix;
    
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 8){
            System.err.printf("Usage: %s <input><output><dbConfig>"
            		+ "<urlFlagPath><cityCode><strRemark><appflagPath><kwflagPath>",
            		getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;                  
		}                  
		    
		String sourcePath = args[0];
		String targetPath = args[1];
		String dbConfig = args[2];
		//URL flag
		String urlflagPath = args[3];//"/user/hive/warehouse/aotain_dim.db/to_url_class/";
		String cityCode = args[4];
		String strRemark = args[5];
		String appflagPath = args[6];
		String kwflagPath = args[7];
		
		Configuration conf = getConf();  
		
		System.out.println("sourcePath: " + sourcePath);
		FileSystem fsSource = FileSystem.get(URI.create(sourcePath),conf);
		Path pathSource = new Path(sourcePath);
	
		if(!fsSource.exists(pathSource))
		{
			return 1;
		}
		    
		//目标目录维护    
		System.out.println("targetPath: " + targetPath);
		FileSystem fsTarget = FileSystem.get(URI.create(targetPath),conf);
		Path pathTarget = new Path(targetPath);
		if(fsTarget.exists(pathTarget))
		{
			fsTarget.delete(pathTarget, true);
			System.out.println("Delete path " + targetPath);
		}
		
		System.out.println("dbconfig:" + dbConfig);
		
		Properties prop = new Properties();   
        InputStream in = new FileInputStream(dbConfig);
        //InputStream inputStream = new FileInputStream(filename);
        
        try {   
            prop.load(in);   
            dbdriver = prop.getProperty("dbdriver").trim();   
            dburl = prop.getProperty("dburl").trim();   
            dbuser = prop.getProperty("dbuser").trim();   
            dbpassword = prop.getProperty("dbpassword").trim();  
            
            postfix = prop.getProperty("postfix").trim();  
            
            System.out.println("######filter url postfix:" + postfix);
            
        } catch (IOException e) {   
            e.printStackTrace();   
        }   
		
		//URL 标签  
        System.out.println("######load url config...");
		CommonFunction.getUrlFlag(urlflagPath,conf);
		System.out.println("######load url config end");
		
		//Cookie 配置
		//getCookieConfig(conf);
		
		//APP 标签
		getAppConfig(appflagPath,conf); 
		
		//Keyword 配置
		getKeywordConfig(kwflagPath,conf); 
		
		CommonFunction.getUrlFliterPostfix(postfix,conf);
		
		conf.set("app.citycode", cityCode);
        conf.set("app.date", strRemark.split("_",-1)[1]);
        
        //Mr job
		Job job = Job.getInstance(conf);
		job.setJobName("DPI [" + strRemark + "]");
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		//FileInputFormat.setInputPathFilter(job,HTTPPathFilter.class);
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		//job.setInputFormatClass(LzoTextInputFormat.class);
		//FileOutputFormat.setCompressOutput(job, true);
	    //FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
	        
		job.setMapperClass(DPIMapper.class);
		//job.setCombinerClass(DPICombiner.class);
		job.setReducerClass(DPIReducer.class);   
		job.setOutputKeyClass(Text.class);
		job.setNumReduceTasks(32);
		job.setOutputValueClass(Text.class);  
		    
		int ret = job.waitForCompletion(true)?0:1;
		
		return ret;  
	}
	
	public static void main(String [] args) throws Exception{
        int mr;
        mr = ToolRunner.run(new Configuration(),new DPIDriver(),args);
        System.exit(mr);
    }
	
	private void getCookieConfig(Configuration conf)
	{
		ResultSet rs= null;
		Connection con = null;
		PreparedStatement ps = null;
		try {
		
			String fieldsplit =dbdriver + "," + dburl + "," + dbuser + "," + dbpassword;
			Class.forName(fieldsplit.split(",")[0]);
		
			con = DriverManager.getConnection(fieldsplit.split(",")[1],fieldsplit.split(",")[2],fieldsplit.split(",")[3]);
			ps = con.prepareStatement("select * from cfg_cookie");
			rs = ps.executeQuery();
			
			//System.out.println("-------------fieldsplit: " + fieldsplit);
			
			String column = "";
	        while(rs.next()){
	        	String text = String.format("%s=%s", rs.getString(1),rs.getString(2));
				column += text +"#";  
			}
			/*for(FieldItem item : confHfile.getColumns())
			{
				String text = String.format("%s=%s=%s", item.FieldName,item.RegExp.length()>0?item.RegExp:"null",item.FieldIndex);
				column += text +"#";  
			}*/
			column  = column.substring(0,column.length() -1 );
			conf.set("app.cookie",column);
		} catch (Exception e) {
				// TODO Auto-generated catch block
			System.out.println("getKeywordConfig:"+e.getMessage());
		} finally{
			try {
				if(rs!=null)
					rs.close();
				if(ps!=null)
					ps.close();
				if(con!=null)
					con.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					System.out.println("getKeywordConfig:"+e.getMessage());
				}
		}
	}
	
	
	
	/**
	 * 获取应用标签配置
	 * @param conf
	 * @throws Exception
	 */
	private void getAppConfig(String appConfigUrl,Configuration conf)
	{
		File file = new File(appConfigUrl);
		
		InputStreamReader in = null;
		//StringBuffer pzFile = new StringBuffer();
		try{
			Map<String,String> map=new HashMap<String,String>();
			if(file.isFile() && file.exists()){//判断是否有文件
			//避免汉字编码问题
			in = new InputStreamReader(new FileInputStream(file),"UTF8");
			BufferedReader buffer = new BufferedReader(in);
			String lineText = "";
			String app = "";
			while((lineText = buffer.readLine()) != null){
				String item[] = lineText.split("\\|",-1);
				app += item[3]+"="+item[0]+";";
				}
			conf.set("app.appflag", app);
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		/*
		ResultSet rs= null;
		Connection con = null;
		PreparedStatement ps = null;
		try
		{
			String fieldsplit = dbdriver + "," + dburl + "," + dbuser + "," + dbpassword;
			Class.forName(fieldsplit.split(",")[0]);
			con = DriverManager.getConnection(fieldsplit.split(",")[1],fieldsplit.split(",")[2],fieldsplit.split(",")[3]);
			ps=con.prepareStatement("select * from to_ref_appcnf");
			String app = "";
			rs=ps.executeQuery();
	        while(rs.next()){
	        		app += rs.getString(4)+"="+rs.getString(1)+";";
	        }
	       
	        app = app.substring(0,app.length() -1 );
			conf.set("app.appflag", app);
		}
		catch(Exception ex)
		{
			System.out.println("getKeywordConfig:"+ex.getMessage());
		}
		finally
		{
			try {
				if(rs!=null)
					rs.close();
				if(ps!=null)
					ps.close();
				if(con!=null)
					con.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					System.out.println("getKeywordConfig:"+e.getMessage());
				}
		}*/
		
	}
	
	
	private  void getKeywordConfig(String filePath,Configuration conf) {
			   String column = "";
				File file=new File(filePath);
				try {
					 InputStreamReader read = new InputStreamReader(new FileInputStream(file),"UTF8");
	                 BufferedReader bufferedReader = new BufferedReader(read);
	                 String lineTxt = null;
	                 while((lineTxt = bufferedReader.readLine()) != null){
	    				column += lineTxt.trim() +"#";  
	                 }
	                 column  = column.substring(0,column.length() -1 );
               	 System.out.println(column);
               	conf.set("app.keyword", column);
				} catch (Exception e) {
					// TODO: handle exception
				}
				  

			  
	}
	
	/**
	 * 获取关键字配置
	 * @param conf
	 */
	private void getKeywordConfig(Configuration conf)
	{
		ResultSet rs= null;
		Connection con = null;
		PreparedStatement ps = null;
			try {
				Class.forName(dbdriver);
				con = DriverManager.getConnection(dburl,dbuser,dbpassword);
				ps = con.prepareStatement("select sdomain,sextract,sencode from cfg_kw");
				rs = ps.executeQuery();
				String column = "";

				while(rs.next()){
					String text = String.format("%s=%s=%s", rs.getString(1),rs.getString(2),rs.getString(3));
					column += text +"#";
				}
		
				column  = column.substring(0,column.length() -1 );
				conf.set("app.keyword", column);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				System.out.println("getKeywordConfig:"+e.getMessage());
			}finally{
				try {
					if(rs!=null)
						rs.close();
					if(ps!=null)
						ps.close();
					if(con!=null)
						con.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						System.out.println("getKeywordConfig:"+e.getMessage());
					}
			}
	}

}

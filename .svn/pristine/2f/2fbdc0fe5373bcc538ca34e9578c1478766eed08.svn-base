package com.aotain.project.szreport;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.mapreduce.LzoTextInputFormat;
import com.aotain.project.szreport.RadiusDataReducer;
public class RadiusDataDriver extends Configured implements Tool {
	
	
	    public int run(String[] args) throws Exception {                  
      /*0:inputfile
       *1��target
       *2:city config //sz���˳���
       * */
	              if (args.length != 3){

	                       System.err.printf("Usage: %s <input><output>",getClass().getSimpleName());

	                       ToolRunner.printGenericCommandUsage(System.err);

	                       return -1;                  

	              }                  
	              
	              Configuration conf =getConf();   
	              String targetPath = args[1];
	              String cityconfig = args[2];
	              
	              System.out.println("targetPath: " + targetPath);
	              System.out.println("citycode: " + cityconfig);
	              
	              FileSystem fsTarget = FileSystem.get(URI.create(targetPath),conf);
	              Path pathTarget = new Path(targetPath);
	              if(fsTarget.exists(pathTarget))
	              {
	            	  fsTarget.delete(pathTarget, true);
	            	  System.out.println("Delete path " + targetPath);
	              }
	              
	              //��ȡ��������
	              String szCityHeader = getSzCityHeader(cityconfig, "755");
	              conf.set("city.data", szCityHeader);
	              
	              Job job = Job.getInstance(conf);
	              
	              job.setJobName("RadiusData");                  

	              job.setJarByClass(getClass());

	              FileInputFormat.addInputPath(job,new Path(args[0]));

	              FileOutputFormat.setOutputPath(job,new Path(args[1]));
	              
	              job.setInputFormatClass(TextInputFormat.class);

	              job.setMapperClass(RadiusDataMapper.class);

	              job.setReducerClass(RadiusDataReducer.class);                  

	              job.setOutputKeyClass(Text.class);

	              job.setOutputValueClass(Text.class);                  

	              return job.waitForCompletion(true)?0:1;                  

	    }


	    private String getSzCityHeader(String cityconfig,String code){
	    	StringBuffer szCityHeader = new StringBuffer("");
	    	try {
				BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(cityconfig),"utf-8"));
				String line =""; //1806364|756|
				while((line = in.readLine()) != null ){
					String[] data = line.split("\\|",-1);
					if(data != null && data.length ==3 && code.equals(data[1])){
						szCityHeader.append(data[0]+"|");
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
	    	return szCityHeader.toString();
	    }
	    
	    public static void main(String[] args)throws Exception{
	         int exitcode = ToolRunner.run(new RadiusDataDriver(), args);
	         System.exit(exitcode);                  
	    }   

}

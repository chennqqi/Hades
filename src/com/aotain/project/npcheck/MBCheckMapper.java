package com.aotain.project.npcheck;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.aotain.common.CommonFunction;

public class MBCheckMapper extends Mapper<NullWritable, OrcStruct, Text, Text>{
	static StructObjectInspector inputOI;
	final static String inputSchema = "struct<areaid:string,username:string,srcip:string,domain:string,"
			+ "url:string,refer:string,opersys:string,opersysver:string,browser:string,"
			+ "browserver:string,device:string,accesstime:bigint,cookie:string,keyword:string,"
			+ "urlclassid:string,referdomain:string,referclassid:string,ua:string,"
			+ "destinationip:string,sourceport:string,destinationport:string>";
;
	public void setup(Context context) throws IOException,
	InterruptedException {
		super.setup(context);
   TypeInfo tfin = TypeInfoUtils
		.getTypeInfoFromTypeString(inputSchema);
     inputOI = (StructObjectInspector) OrcStruct
		.createObjectInspector(tfin);
}
	protected void map(NullWritable meaningless, OrcStruct orc, Context context) throws java.io.IOException ,InterruptedException {
		
		 
	
			List<Object> ilst = inputOI.getStructFieldsDataAsList(orc);
			  if( ilst.size() != 21){
				  return;
			  }

		  	 String[] items=new String[22];
		      for(int i=0;i<ilst.size();i++){
		    	items[i]=ilst.get(i).toString();
		      }
		 
		  try {   
			  String systemour=items[6].toLowerCase().trim();
		      boolean sysflag=systemour.equals("android")||systemour.equals("ios");
		      String sys = items[6]+","+items[7]+","+items[10];
			  if(sysflag){
				  //6 opersys //7opersysver //10 device
				  boolean flag = StringUtils.isNotEmpty(items[6])&&StringUtils.isNotEmpty(items[7])&&StringUtils.isNotEmpty(items[10]);
				  boolean sysverflag = filterSysver(items[6],items[7]);
				  if(flag && sysverflag){
					  if(!sysverflag){
						  sys = items[6];  
					  }
					  context.write(new Text(items[1]), new Text(sys)); 
				  }
			  }
		  }catch (Exception e) {
			  
		  }
	};
	
	
	public  boolean filterSysver(String sys,String version){
		String regEx = "";
		if(sys.toLowerCase().equals("android")){
			regEx = "[2-5]\\.[0-9]\\.[0-9]|[2-5]\\.[0-9]";
		}else if(sys.toLowerCase().equals("ios")){
			regEx = "[2-8]\\.[0-9]\\.[0-9]|[2-8]\\.[0-9]";
		}
		
		String ret = CommonFunction.findByRegex(version, regEx, 0);
		if(ret == null){
			return false;
		}
		return true;
	}
	
	public static void main(String[] args) {
		String systemour="".toLowerCase().trim();
	}
}

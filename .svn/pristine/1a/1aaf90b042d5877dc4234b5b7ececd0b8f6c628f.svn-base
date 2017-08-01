package com.aotain.project.npcheck;

import java.io.IOException; 
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aotain.common.CommonFunction;
import com.aotain.common.ObjectSerializer;

public class NPointCheckMapper extends Mapper<NullWritable, OrcStruct, Text, Text>{
	//int index;
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
	 @Override
      public void map(NullWritable meaningless, OrcStruct orc, Context context) throws IOException, InterruptedException{                         
		  
		  	
		
		
		  try
		  
		  {
			 
		      String column = context.getConfiguration().get("column");
		  	List<Object> ilst = inputOI.getStructFieldsDataAsList(orc);
		  	 if( ilst.size() != 21){
				  return;
			  }
		  	 String[] items=new String[22];
		      for(int i=0;i<ilst.size();i++){
		    	  items[i]=ilst.get(i).toString();
		      }
	        
      
			  Map<String,String> webconfigmap=( HashMap<String,String>) ObjectSerializer
		            .deserialize(column);
		  String systemour=items[6].toLowerCase().trim();  
		  String systemvers=items[7];
		  boolean sysflag=false;
		  if(systemour.equals("windows")){
			
			  Pattern pattern = Pattern.compile("NT[2-6]\\.[0-9]");
		      Matcher  matcher = pattern.matcher(systemvers.replace(" ", ""));

		      if(matcher.find())
		    	  sysflag=true; 

		  }else if(systemour.equals("macintosh")){
			  Pattern pattern = Pattern.compile("(10_[0-9]_[0-9])|(10_10_[0-9])|(10\\.[0-9])|(10\\.10)");
		      Matcher  matcher = pattern.matcher(systemvers.replace(" ", ""));

		      if(matcher.find())
		    	  sysflag=true; 
		    

		  }
		//  boolean sysflag=systemour.equals("windows")||systemour.equals("macintosh")||systemour.equals("x11")||systemour.equals("linux");
			
		     String domain=items[3];
		     String cookieValue="null";
			 String sys="null";
		    
			 if(sysflag){
				    sys=items[6]+items[7];
                    int index=-1;
				     if(domain.contains("qq.com")){
				    	 index=2;
				    	 cookieValue= parseCookie(cookieValue, webconfigmap, items, "qq.com");
				    	 cookieValue=CommonFunction.getQQNumber(cookieValue);
				     }else if(domain.contains(".360.c")){
				    	 index=0;
				    	 cookieValue= parseCookie(cookieValue, webconfigmap, items, ".360.c");
				     }else {
				    	 index=1;
				    	 cookieValue= parseCookie(cookieValue, webconfigmap, items,domain );
				     }
				     if(!"null".equals(cookieValue)||!"null".equals(sys))
						  context.write(new Text(items[1]),new Text(items[3]+","+cookieValue+","+index+","+sys));   
		     }

			 
		 
		     }
     	 catch(Exception e)
     	 {
     		 e.printStackTrace();
     	 
     	 }
      }  
	  
	  private String parseCookie(String cookieValue,Map <String,String>webconfigmap,String[] items,String key){
             
	    	 String webconfigStr=webconfigmap.get(key);
		    if(webconfigStr!=null){
			    	 String[] webconfigs=webconfigStr.split("=",3);
			    	 String [] configparse=webconfigs[1].split("@");
			    	 int position=Integer.parseInt(configparse[2]);
			    	 String startname=configparse[0];
					 String endtname=configparse[1];
					  //index=Integer.parseInt(webconfigs[2]);
					 if(items[position].length()>0&&items[position].indexOf(startname)!=-1){
						  cookieValue=items[position].substring(items[position].indexOf(startname),items[position].length());
						  if(cookieValue.indexOf(endtname)!=-1)
						  cookieValue=cookieValue.substring(0,cookieValue.indexOf(endtname));
						  cookieValue=cookieValue.split("=")[1];
					 }
		    	 }
		  return cookieValue;
	  }
	 
	  public static void main(String[] args) {
	      String ss="1";
	      System.out.println(ss.length());
		// String s="pgv_info=ssid=s6263183172";
		// String s2=s.split("=")[1];
		 //System.out.println(s2);
		String s="ttgg=oopp;cna=225;cna=000";
		String cookieValue=s.substring(s.indexOf("cna"),s.length());
		System.out.println(cookieValue);
		if(cookieValue.indexOf(";")!=-1)
		cookieValue=cookieValue.substring(0,cookieValue.indexOf(";"));
		 cookieValue=cookieValue.split("=")[1];
		 System.out.println(cookieValue);
	 }
}

package com.aotain.project.mbanalysis;

import java.io.IOException; 
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PCAnalysisResultReducer extends Reducer<Text,Text,Text,Text> {
	 @Override
     public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException        {
		 
		  
		    String date = context.getConfiguration().get("date");
		 	String vkey = key.toString()+",";
		 	/*int max=0;*/
		 	try{
		 		//将网站分为三组，一组为常规的过滤网站放入webmap中，一组为qq类型网站，放入qqmap中；一组为统计360类网站，放入360map中
		 	 Map<String,Set<String>>webmap=new HashMap<String, Set<String>>();
		 	Map<String,Set<String>>qqmap=new HashMap<String, Set<String>>();
		 	Map<String,Set<String>>map360=new HashMap<String, Set<String>>();
		 	Set<String>systemset=new HashSet<String>();
		    for(Text value:values){
		 		 String truevalue=value.toString();
		 		 if(truevalue.split(",")!=null&&truevalue.split(",").length==4){
				 		 String mapkey=truevalue.split(",")[0];
				 		 String cookie=truevalue.split(",")[1];
				 		 String operasys=truevalue.split(",")[3];
				 		 if(!"null".equals(operasys)&& !operasys.contains("systemerror"))
				 		 systemset.add(operasys);
				 		 if(!"null".equals(cookie))
				 		 {
				 		 int index=Integer.parseInt(truevalue.split(",")[2]);
				 		if(index==1){
					 		 if(webmap.get(mapkey)!=null){
					 			 Set<String> set=webmap.get(mapkey);
					 		     set.add(cookie);
					 		 }else{
					 			 Set<String>set=new HashSet<String>();
					 			 set.add(cookie);
					 			 webmap.put(mapkey, set);
					 		 }
				 		 }else if(index==2){
				 			 mapkey="qqcount";
				 			 if(qqmap.get(mapkey)!=null){
					 			 Set<String> set=qqmap.get(mapkey);
					 		     set.add(cookie);
					 		 }else{
					 			 Set<String>set=new HashSet<String>();
					 			 set.add(cookie);
					 			 qqmap.put(mapkey, set);
					 		 }
				 		 }else if(index==0){
				 			if(map360.get(mapkey)!=null){
					 			 Set<String> set=map360.get(mapkey);
					 		     set.add(cookie);
					 		 }else{
					 			 Set<String>set=new HashSet<String>();
					 			 set.add(cookie);
					 			 map360.put(mapkey, set);
					 		 }
				 		 }
		 		 }
			  }
		 	 }
		 	 String webname="";
		 	 int webmax=0;
		 	 //统计常规类型中账户数
		 	 Iterator<Entry<String, Set<String>>>webit=webmap.entrySet().iterator();
		 	 while(webit.hasNext()){
		 		Entry<String, Set<String>> en=webit.next();
		 		if(webmax<en.getValue().size()){
		 			webmax=en.getValue().size();
		 		    webname=en.getKey();
		 		    }
		 	 }
		 	 int qqmax=0;
		 	 if(qqmap.get("qqcount")!=null)
		 	 qqmax=qqmap.get("qqcount").size();
		 	 String qqname="qqcount";
		 	// Iterator<Entry<String, Set<String>>>qqit=qqmap.entrySet().iterator();
		 	 //统计qq类型中账户数
		 	/* while(qqit.hasNext()){
		 		Entry<String, Set<String>> en=qqit.next();
		 		if(qqmax<en.getValue().size()){
		 			qqmax=en.getValue().size();
		 			qqname=en.getKey();
		 		   }
		 	 }*/
		 	int max360=0;
		 	String name360="";
		 	//统计360类型中账户数
		 	 Iterator<Entry<String, Set<String>>>it360=map360.entrySet().iterator();
		 	 while(it360.hasNext()){
		 		Entry<String, Set<String>> en=it360.next();
		 		if(max360<en.getValue().size()){
		 			max360=en.getValue().size();
		 			name360=en.getKey();
		 		   }
		 	 }
		 	 int countmax=0;//最终统计数
		 	 
		 	 if(qqmax!=0||max360!=0){
		 		 if(qqmax==0)//如果无qq账户数则直接取360账户数放入最终统计数中
		 			 countmax=max360;
		 		 else if(max360==0)//如果360账户数为0则直接取qq账户数
		 			 countmax=qqmax;
		 		 else{
		 			 countmax=qqmax>max360?qqmax:max360;//取qq与360账户数中最大者，放入最终统计数中
		 		 }
		 			
		 	 }
		 	 if(webmax!=0){
		 	     if(countmax!=0)
		 	     countmax=webmax>countmax?countmax:webmax;//将最终统计数与常规统计比较，取二者之中小的那一方
		 	     else
		 	    countmax=webmax;
		 	 }
		 	 
		 	 int systemcount=systemset.size();
		 	countmax=countmax>systemcount?countmax:systemcount;
		 	 /* max=webmax>=qqmax?qqmax:webmax;
		 	 max=max>=max360?max360:max;*/
		 	 vkey+=date+","+webmax+","+webname+","+qqmax+","+qqname+","+max360+","+name360+","+systemcount+","+countmax+",";
		 	if(countmax!=0)
		 	context.write(new Text(vkey), new Text(""));
		 	}catch(Exception e){
		 		e.printStackTrace();
		 	}
		 	
     } 
}
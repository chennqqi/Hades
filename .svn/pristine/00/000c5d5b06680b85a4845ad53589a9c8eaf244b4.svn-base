package com.aotain.project.sada;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

public class UserIdentifierSReducer   extends Reducer<Text,Text,Text,Text>{
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException     
	{
		Text v = new Text();
		 HashMap<String,HashMap<String, Integer>> map_field = new HashMap<String,HashMap<String, Integer>>();
		 final Counter counter = context.getCounter("Sensitive", "cts");  
		StringBuffer sb= new StringBuffer();
		StringBuffer sb1= new StringBuffer();
		StringBuffer sb2= new StringBuffer();
		StringBuffer sb3= new StringBuffer();
		StringBuffer sb4= new StringBuffer();
		StringBuffer sb5= new StringBuffer();

		String[] vkey = key.toString().trim().split("\\|",-1);
		Pattern p = Pattern.compile("^[0-9]+$", Pattern.CASE_INSENSITIVE);
		 
		try {
			EncryptUtil des = new EncryptUtil("aotain");
			
		for(Text t : values)
		{
			String[] items = t.toString().trim().split("\\|",-1);
	 		Matcher m = p.matcher(items[2]);
    		if(!m.find()){
    			continue;
    		}
	   		 HashMap<String,Integer> map = map_field.get(items[0]);
			 if(map == null)
			 {
				 map = new HashMap<String,Integer>();
	    		 map.put(items[1],Integer.parseInt(items[2]));
				 map_field.put(items[0], map);
			 }
			 else
			 {
				 if(map.get(items[1]) == null)
	    			 map.put(items[1],Integer.parseInt(items[2]));
				 else
					 map.put(items[1],map.get(items[1])+Integer.parseInt(items[2]));
			 }
			 
	 }
		String[] fields=new String[]{"Mail","IMEI","MAC","IDFA","Phone"};
		
        for(String temp:fields)
        {
    		HashMap<String, Integer> map = map_field.get(temp);
    		if(map != null){
        		for(int j=0; j < 30; j++)  
        		{
	        		Iterator<String> it_temp = map.keySet().iterator();
	        		Integer max = 0;
	        		String s="",skey="";
	        		while(it_temp.hasNext())
	        		{
	        			s = it_temp.next();
	        			if(map.get(s) > max)
	        			{
	        				max = map.get(s);
	        				skey = s;
	        			}
	        		}
	        		if(map.size()>0 && skey!="")
	        		{
	        			if("Mail".equals(temp)){
	        				sb1.append("\""+skey+"\",");
	        			}else if("IMEI".equals(temp)){
	        				sb2.append("\""+skey+"\",");
	        			}else if("MAC".equals(temp)){
	        				sb3.append("\""+skey+"\",");
	        			}else if("IDFA".equals(temp)){
	        				sb4.append("\""+skey+"\",");
	        			}else if("Phone".equals(temp)){
	        				sb5.append("\""+des.encrypt(skey)+"\",");
	        			}
	        			map.remove(skey);
	        		}else
	        			break;
        		}  
    		}
        
        }
		
		if (! ((sb5.toString() == null) || (sb5.toString().length() == 0))) {
			sb5.insert(0, "\"Phone\":[").delete(sb5.length()-1, sb5.length()).append("]");
			sb.append(sb5).append(",");
		}
		
		if (! ((sb1.toString() == null) || (sb1.toString().length() == 0))) {
			sb1.insert(0, "\"Mail\":[").delete(sb1.length()-1, sb1.length()).append("]");
			sb.append(sb1).append(",");
		}
		
		if (! ((sb2.toString() == null) || (sb2.toString().length() == 0))) {
			sb2.insert(0, "\"IMEI\":[").delete(sb2.length()-1, sb2.length()).append("]");
			sb.append(sb2).append(",");
		}
		
		if (! ((sb3.toString() == null) || (sb3.toString().length() == 0))) {
			sb3.insert(0, "\"MAC\":[").delete(sb3.length()-1, sb3.length()).append("]");
			sb.append(sb3).append(",");
		}
		
		if (! ((sb4.toString() == null) || (sb4.toString().length() == 0))) {
			sb4.insert(0, "\"IDFA\":[").delete(sb4.length()-1, sb4.length()).append("]");
			sb.append(sb4).append(",");
		}
		
		counter.increment(1);
		v.set(String.format("%s\t%s|%s",vkey[1]+"_"+counter.getValue(),vkey[0],sb.insert(0, "{").delete(sb.length()-1, sb.length()).append("}")));	
		context.write(v, new Text(""));
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

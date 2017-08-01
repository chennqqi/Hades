package com.aotain.project.sada;

import java.io.IOException; 
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aotain.common.CommonFunction;

public class UserAttMapper extends Mapper<LongWritable,Text,Text,Text>{
	  @Override
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{                         
		  	
		  String fieldsplit = context.getConfiguration().get("fieldsplit");
		  String date = context.getConfiguration().get("date");
		  long limitday = context.getConfiguration().get("limitday").equals("null")?0:Long.parseLong(context.getConfiguration().get("limitday"));
		  String rowkey = context.getConfiguration().get("rowkey");
		  String column = context.getConfiguration().get("column");
		  String filter = context.getConfiguration().get("filter");
		  
		  String headkey = "";
		  String vkey = "";
		  String regexp = ""; 
		  String[] items;
		  String[] temps;
		  String[] arr_cls;
		  Pattern p;
		  Matcher m;
		  String vret = value.toString();
		  
		  try
		  {
			  
			if (vret.contains(fieldsplit)) 
			{
				items = vret.split("\\" + fieldsplit, -1);

				if (!filter.equals("null")) {
					temps = filter.split("#");
					for (int i = 0; i < temps.length; i++) {
						arr_cls = temps[i].split("=");
						if (!items[Integer.parseInt(arr_cls[0])]
								.matches(arr_cls[1]))
							return;
					}
				}

				arr_cls = rowkey.split("=");
				regexp = arr_cls[2];
				int index = Integer.parseInt(arr_cls[0]);

				if (items.length <= index || items[index].trim().length() < 1)
					return;

				if (!regexp.equals("null") && !validateUser(items[index])) {
					return;
				}

				headkey += items[index] + ",";


				temps = column.split("#");
				for (int i = 0; i < temps.length; i++) {
					arr_cls = temps[i].split("=");
					regexp = arr_cls[2];
					index = Integer.parseInt(arr_cls[0]);
					if (items.length <= index
							|| items[index].trim().length() < 1)
						continue;

					if (!regexp.equals("null")) {
						p = Pattern.compile(regexp);
						m = p.matcher(items[index]);
						if (!m.find())
							continue;
					}

					vkey = headkey + arr_cls[1] + "," + items[index];
					context.write(new Text(vkey.trim()), new Text("1,0," + date));
				}
			} else {
				items = vret.split(",", -1);
				regexp = rowkey.split("=")[2];
				if (!regexp.equals("null") && !validateUser(items[0])) {
					return;
				}
				
				if(items.length == 5){
					if (limitday == 0) {
						context.write(new Text(items[0] + "," + items[1] + ","
								+ items[2]), new Text(items[3] + ",0," + items[4]));
					} else {

						SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
						Date date1 = format.parse(items[4].substring(0, 8));
						Date date2 = format.parse(date);
						long day1 = date1.getTime();
						long day2 = date2.getTime();
						long days = (day2 - day1) / 1000 / 3600 / 24;

						if (days < limitday) {
							context.write(new Text(items[0] + "," + items[1] + ","
									+ items[2]),
									new Text(items[3] + ",0," + items[4]));
						}
				}
				}else if(items.length == 7){
					if (limitday == 0) {
						context.write(new Text(items[0] + "," + items[1] + ","
								+ items[2]), new Text(items[3] + "," + items[4]+ "," + items[5]));
					} else {

						SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
						Date date1 = format.parse(items[5].substring(0, 8));
						Date date2 = format.parse(date);
						long day1 = date1.getTime();
						long day2 = date2.getTime();
						long days = (day2 - day1) / 1000 / 3600 / 24;

						if (days < limitday) {
							context.write(new Text(items[0] + "," + items[1] + ","
									+ items[2]),
									new Text(items[3] + "," + items[4] + "," + items[5]));
						}
				}
				}
			}
		  }
     	 catch(Exception e)
     	 {;}
      }     
	  
	  private static boolean validateUser(String username)
		{
			boolean ret = false;
			
			String aa = CommonFunction.findByRegex(username, "^([a-zA-Z0-9_\\-\\.]{1,50})@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.)|(([a-zA-Z0-9\\-]+\\.)+))(com|cn|net|com.cn|org|gd)(\\]?)$", 0);
			if(aa == null)
			{
				if(username.indexOf("IPCYW")==0)
				{		
					ret = true;
				}
			}
			else
			{
				ret = true;
			}
			
			return ret;
		}
}

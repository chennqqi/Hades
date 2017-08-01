package com.aotain.dim;

import java.io.IOException; 
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TerminalAttMapper extends Mapper<LongWritable,Text,Text,Text>{
	  @Override
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{                         
		  	
		  String kvsplit = context.getConfiguration().get("kvsplit");//keyvalue分隔符
		  String fieldsplit = context.getConfiguration().get("fieldsplit");//列属性分隔符
		  String date = context.getConfiguration().get("date");
		  String rowkey = context.getConfiguration().get("rowkey");
		  String column = context.getConfiguration().get("column");
		  
		  String headkey = "";
		  String vkey = "";
		  String regexp = "";
		  String[] items;
		  String[] arr_cls;

		  try
		  {
				  //源文件据处理
				  items = value.toString().split("\\"+fieldsplit,-1);
	
				  for(int i = 0;i < items.length; i++)
				  {
						 arr_cls = items[i].split("\\"+kvsplit,2);

						 if(arr_cls.length>1 && rowkey.contains(arr_cls[0]+"=") && arr_cls[1].trim().length()>0)
						 {
							 regexp = rowkey.split("=",2)[1];
							 if(!regexp.equals("null") && !arr_cls[1].matches(regexp))
							 {
								 return;
							 }
							 headkey+=arr_cls[1]+",";
							 break;
						 }
				  }
				  
				  if(headkey.length()<1)
					  return;
				  
				  //列与正则
				  String[] temps = column.split("#",-1);
				  HashMap<String,String> map_cls = new HashMap<String,String>();
				  for(int i = 0;i < temps.length; i++)
				  {
					  arr_cls = temps[i].split("=",2);
					  map_cls.put(arr_cls[0],arr_cls[1]);
				  }
				  
				  for(int i = 0;i < items.length; i++)
				  {
						 arr_cls = items[i].split("\\"+kvsplit,2);
						 if(arr_cls.length>1 && map_cls.get(arr_cls[0])!=null && arr_cls[1].trim().length()>0)
						 {
							 regexp = map_cls.get(arr_cls[0]);
							 if(!regexp.equals("null") && !arr_cls[1].matches(regexp))
							 {
								 continue;
							 }
							 String head = arr_cls[0].trim();
							 String ret = arr_cls[1].trim();
							 if(head.equals("价格"))
							 {
								 float price = 0;
								 try
								 {
									 price = Float.parseFloat(ret);
									 if(price>=0 && price<500)
										 ret = "0~499元";
									 else if(price>=500 && price<1000)
										 ret = "500~999元";
									 else if(price>=1000 && price<1500)
										 ret = "1000~1499元";
									 else if(price>=1500 && price<2000)
										 ret = "1500~1999元";
									 else if(price>=2000 && price<2500)
										 ret = "2000~2499元";
									 else if(price>=2500 && price<3000)
										 ret = "2500~2999元";
									 else if(price>=3000 && price<3500)
										 ret = "3000~3499元";
									 else if(price>=3500)
										 ret = "3500元以上";
								 }
								 catch(Exception e)
								 {
									 ;
								 }
								 vkey = headkey+"价格段"+","+ret;
					             context.write(new Text(vkey.trim()),new Text("1,"+date)); 
							 }
							 else if(head.equals("主屏尺寸"))
							 {
								 float screen = 0;
								 try
								 {
									 screen = Float.parseFloat(ret.split("，", 2)[0].split(";", 2)[0].replace("..", ".").replace("英寸", ""));
									 if(screen>=6.0)
										 ret = "6.0英寸以上";
									 else if(screen>=5.0 && screen<6.0)
										 ret = "5.1-5.9英寸";
									 else if(screen==5.0)
										 ret = "5.0英寸";
									 else if(screen>=4.5 && screen<5.0)
										 ret = "4.5-4.9英寸";
									 else if(screen>=4.0 && screen<4.5)
										 ret = "4.1-4.4英寸";
									 else if(screen==4.0)
										 ret = "4.0英寸";
									 else if(screen>=3.0 && screen<4.0)
										 ret = "3.0-3.9英寸";
									 else if(screen<3.0)
										 ret = "2.9英寸以下";
								 }
								 catch(Exception e)
								 {
									 ;
								 }
								 vkey = headkey+"主屏尺寸段"+","+ret;
					             context.write(new Text(vkey.trim()),new Text("1,"+date)); 
							 }
							 else if(head.equals("屏幕尺寸"))
							 {
								 head = "主屏尺寸";
								 float screen = 0;
								 try
								 {
									 screen = Float.parseFloat(ret.split("，", 2)[0].split(";", 2)[0].replace("..", ".").replace("英寸", ""));
									 if(screen>=10.1)
										 ret = "11英寸及以上 ";
									 else if(screen>9.7 && screen<=10.1)
										 ret = "10.1英寸";
									 else if(screen>7.9 && screen<=9.7)
										 ret = "8-9.7英寸";
									 else if(screen>7.0 && screen<=7.9)
										 ret = " 7.9英寸";
									 else if(screen>6.0 && screen<=7.0)
										 ret = "7英寸 ";
									 else if(screen<=6.0)
										 ret = "6英寸及以下";
								 }
								 catch(Exception e)
								 {
									 ;
								 }
								 vkey = headkey+"主屏尺寸段"+","+ret;
					             context.write(new Text(vkey.trim()),new Text("1,"+date)); 
							 }
							 else if(head.contains("像素密度"))
							 {
								 float ppi = 0;
								 try
								 {
									 ppi = Float.parseFloat(ret.replace("ppi", ""));
									 if(ppi>=400)
										 ret = "400ppi以上";
									 else if(ppi>=350 && ppi<400)
										 ret = "350-399ppi";
									 else if(ppi>=300 && ppi<350)
										 ret = "300-349ppi";
									 else if(ppi>=250 && ppi<300)
										 ret = "250-299ppi";
									 else if(ppi>=0 && ppi<250)
										 ret = "0-249ppi";
								 }
								 catch(Exception e)
								 {
									 ;
								 }
								 vkey = headkey+"像素密度段"+","+ret;
					             context.write(new Text(vkey.trim()),new Text("1,"+date)); 
							 }
							 vkey = headkey+head+","+arr_cls[1];
				             context.write(new Text(vkey.trim()),new Text("1,"+date)); 
						 } 
				  }
		  }
     	 catch(Exception e)
     	 {;}
      }        
}

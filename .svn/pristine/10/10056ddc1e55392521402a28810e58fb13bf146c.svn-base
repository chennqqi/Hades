package com.aotain.project.sada;

import java.io.IOException; 
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aotain.common.CommonFunction;

public class UserAttOrcMapper extends Mapper<NullWritable,OrcStruct,Text,Text>{
	  
	final static String inputSchema = "struct<areaid:string,username:string,srcip:string,domain:string,"
			+ "url:string,refer:string,opersys:string,opersysver:string,browser:string,"
			+ "browserver:string,device:string,accesstime:bigint,cookie:string,keyword:string,"
			+ "urlclassid:string,referdomain:string,referclassid:string,ua:string,"
			+ "destinationip:string,sourceport:string,destinationport:string>";
	static StructObjectInspector inputOI;

		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			TypeInfo tfin = TypeInfoUtils
					.getTypeInfoFromTypeString(inputSchema);
			inputOI = (StructObjectInspector) OrcStruct
					.createObjectInspector(tfin);
		}
		
	@Override
      public void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException{                         
		  	
		  String fieldsplit = context.getConfiguration().get("fieldsplit");
		  String date = context.getConfiguration().get("date");
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
		  
		  List<Object> ilst = inputOI.getStructFieldsDataAsList(value);
		  String vret="";
		  if( ilst.size() != 21){
			  return;
		  }
		  for(int i=0;i< ilst.size();i++){
			  String lt = ilst.get(i)+fieldsplit;
			   vret+=lt;
		  }
		  
		  try
		  {
			  if(vret.contains(fieldsplit)) 
			  {
		
				  items = vret.split("\\"+fieldsplit,-1);
				  
				  if(!filter.equals("null"))
				  {
					  temps = filter.split("#");
					  for(int i = 0;i < temps.length; i++)
					  {
						  arr_cls = temps[i].split("=");
					  	  if(!items[Integer.parseInt(arr_cls[0])].matches(arr_cls[1]))
					  			return;
					  }
				  }
	
				  arr_cls = rowkey.split("=");
				  regexp = arr_cls[2];
				  int index = Integer.parseInt(arr_cls[0]);
				  
				  if(items.length<=index || items[index].trim().length()<1)
					  return;
	
				  if(!regexp.equals("null") && !validateUser(items[index]))
				  {
					  return;
				  }
				  
				  headkey+=items[index]+",";
				  
				  temps = column.split("#");
				  for(int i = 0;i < temps.length; i++)
				  {
					  arr_cls = temps[i].split("=");
					  regexp = arr_cls[2];
					  index = Integer.parseInt(arr_cls[0]);
					  if(items.length<=index || items[index].trim().length()<1)
						  continue;
					  
					  if(!regexp.equals("null"))
					  {
						  p = Pattern.compile(regexp);
						  m = p.matcher(items[index]);
						  if(!m.find())
							  continue;
					  }
	
					  vkey = headkey+arr_cls[1]+","+items[index];
					  context.write(new Text(vkey.trim()),new Text("1,0,"+date)); 
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

package com.aotain.project.sada;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.aotain.common.CommonFunction;
import com.aotain.common.ObjectSerializer;


public class AppTagDriver extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		if(args.length != 4 )
		{
			System.out.print("args error!");
			return -1;
		}
		
    	Configuration conf = new Configuration(); 
		
		String inputPath = args[0];
		String appPath = args[1];
		String targetPath = args[2];
		String date = args[3];
		Map<String,String> domains=new HashMap<String,String>();
		Map<String,String> urls=new HashMap<String,String>();
		Map<String,String> uas=new HashMap<String,String>();
		 FileSystem fsPhone = FileSystem.get(URI.create(appPath),conf); 
		FSDataInputStream in = null;
		try {
			Path path = new Path(appPath);
			if (fsPhone.exists(path)) {
				for (FileStatus file : fsPhone.listStatus(path)) {
					in = fsPhone.open(file.getPath());
					BufferedReader bis = new BufferedReader(
							new InputStreamReader(in, "UTF8"));
					String line = "";
					while ((line = bis.readLine()) != null) {
						String[] arr = line.split(",");
						if("domain".equals(arr[3])){
							domains.put(arr[2], arr[1]+","+arr[0]);
						}else if("url".equals(arr[3])){
							urls.put(arr[4]+","+arr[2], arr[1]+","+arr[0]);
						}else if("ua".equals(arr[3])){
							uas.put(arr[2].trim().toLowerCase(), arr[1]+","+arr[0]);
						}
					}
				}
			} else {
				System.out.println("not exist file !");
			}
		} finally {
			if (in != null)
				IOUtils.closeStream(in);
		}
		
		conf.set("domains", ObjectSerializer.serialize((Serializable) domains));
		conf.set("urls", ObjectSerializer.serialize((Serializable) urls));
		conf.set("uas", ObjectSerializer.serialize((Serializable) uas));
	
		System.out.println(domains.size());
		System.out.println(urls.size());
		System.out.println(uas.size());
		FileSystem fsTarget = FileSystem.get(URI.create(targetPath),conf);
        Path pathTarget = new Path(targetPath);
        if(fsTarget.exists(pathTarget))
        {
      	  	fsTarget.delete(pathTarget, true);
        }
        conf.set("date",date);
        System.out.println(date);
        Job job = Job.getInstance(conf);
        job.setJobName("APP Fus File[" + date + "]");                    
        job.setJarByClass(getClass());
        job.setInputFormatClass(OrcNewInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(AppTagMapper.class);
        job.setReducerClass(AppTagReducer.class);
        job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class); 
	    
	    FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job,new Path(targetPath));         

        return job.waitForCompletion(true)?0:1;  
	}
	
	public static void main(String[] args)throws Exception{
        int exitcode = ToolRunner.run(new AppTagDriver(), args);
        System.exit(exitcode);                  
   }   
}

class AppTagMapper extends Mapper<NullWritable,OrcStruct,Text,Text>{
	 
	final static String inputSchema = "struct<areaid:string,username:string,srcip:string,domain:string,"
			+ "url:string,refer:string,opersys:string,opersysver:string,browser:string,"
			+ "browserver:string,device:string,accesstime:bigint,cookie:string,keyword:string,"
			+ "urlclassid:string,referdomain:string,referclassid:string,ua:string,"
			+ "destinationip:string,sourceport:string,destinationport:string>";
	static StructObjectInspector inputOI;
	private static Map<String,String> domains=new HashMap<String,String>();
	private static Map<String,String> urls=new HashMap<String,String>();
	private static Map<String,String> uas=new HashMap<String,String>();
	
	
	public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			TypeInfo tfin = TypeInfoUtils
					.getTypeInfoFromTypeString(inputSchema);
			inputOI = (StructObjectInspector) OrcStruct
					.createObjectInspector(tfin);
			String str1 = context.getConfiguration().get("domains");
			domains  = ( HashMap<String,String>) ObjectSerializer
			            .deserialize(str1);
			 String str2 = context.getConfiguration().get("urls");
			 urls  = ( HashMap<String,String>) ObjectSerializer
			            .deserialize(str2);
			 String str3 = context.getConfiguration().get("uas");
			 uas  = ( HashMap<String,String>) ObjectSerializer
			            .deserialize(str3);
					
		}

	 
	public void map(NullWritable key, OrcStruct value, Context context) 
			throws IOException, InterruptedException{
		String sUserName = "";
		boolean isDomain = false;
		boolean isURL = false;
		boolean isUA = false;
		boolean f1 = false;
		boolean f2 = false;
		
		try
      {		
			  List<Object> ilst = inputOI.getStructFieldsDataAsList(value);
			  if( ilst.size() != 21){
				  return;
			  }
			  
			String cell =ilst.get(1).toString();
			if (!validateUser(cell)) {
				sUserName = cell;
				String domain = ilst.get(3).toString().trim();
				if (domain.length() > 0 && domain != null)  isDomain = true;
				
				String url = ilst.get(4).toString().trim();
				if (url.contains("%")){
					url = url.replaceAll("%(?![0-9a-fA-F]{2})", "%25");  
					url = java.net.URLDecoder.decode(url, "utf-8");
				}
				if (url.length() > 0 && url != null)  isURL = true;
				
				String ua =  ilst.get(17).toString().trim().toLowerCase();
				if (ua.length() > 0 && ua != null)  isUA = true;
			
				
				 if(isDomain){
						for (Map.Entry<String, String> entry : domains.entrySet()) {
							 String k = entry.getKey();
							 String v = entry.getValue();
							 if(domain.contains(k)){
								 context.write(new Text(sUserName+","+v), new Text("1"));
								 f1 = true;
								 break;
							 }
						}
				 }
				 
				 if(!f1 && isURL ){
					 for (Map.Entry<String, String> entry : urls.entrySet()) {
						 String[] ks = entry.getKey().split(",");
						 String v = entry.getValue();
						 if(isDomain && domain.contains(ks[0])){
							 if(url.contains(ks[1])){
								 context.write(new Text(sUserName+","+v), new Text("1"));
								 f2 = true;
								 break;
							 }
						 }
					}
				 }
				 
				 if(!f2 && isUA){
						for (Map.Entry<String, String> entry : uas.entrySet()) {
							 String k = entry.getKey();
							 String v = entry.getValue();
							 if(ua.contains(k)){
								 context.write(new Text(sUserName+","+v), new Text("1"));
								 break;
							 }
						}
				 }
				}
		}
		catch (Exception e)  {;}
	}
	
	 
	 private static boolean validateUser(String username){
		boolean ret = false;
		String aa = CommonFunction.findByRegex(username, "^([a-zA-Z0-9_\\-\\.]{1,50})@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.)|(([a-zA-Z0-9\\-]+\\.)+))(com|cn|net|com.cn|org|gd)(\\]?)$", 0);
		if(aa == null)
		{
			ret = true;
		}
		
		return ret;
	}
	 
	 public static String findByRegex(String str, String regEx, int group)
	 	{
	 		String resultValue = null;
	 		if ((str == null) || (regEx == null) || ((regEx != null) && ("".equals(regEx.trim())))) 
	 			return resultValue;
	 		
	 		
	 		Pattern p = Pattern.compile(regEx);
	 		Matcher m = p.matcher(str);

	 		boolean result = m.find();
	 		if (result)
	 		{
	 			resultValue = m.group(group);
	 		}
	 		return resultValue;
	 	}
	 
}


class AppTagReducer extends Reducer<Text,Text,Text,Text>{
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException     
	{
		Text v = new Text();
		int count = 0;
		String date = context.getConfiguration().get("date");
		
		for(Text t : values)
		{
			count += 1;
		}
		
		v.set(String.format("%s,%s,%s",key,count,date));
		context.write(v, new Text(""));
	}

}


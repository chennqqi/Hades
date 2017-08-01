package com.aotain.project.sada;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class ParseIdentifierDriver extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length!=3)
		{
			System.out.print("args error!");
			return -1;
		}
		
    	Configuration conf = new Configuration(); 
		
		String inputPath = args[0];
		String targetPath = args[1];
		String date = args[2];
	
		FileSystem fsTarget = FileSystem.get(URI.create(targetPath),conf);
        Path pathTarget = new Path(targetPath);
        if(fsTarget.exists(pathTarget))
        {
      	  	fsTarget.delete(pathTarget, true);
        }
        
        conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive",true);
        conf.set("date",date);
        
        Job job = Job.getInstance(conf);
        job.setJobName("ParseIdentifier File[" + date + "]");                    
        job.setJarByClass(getClass());

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(ParseIdentifierMapper.class);
        job.setReducerClass(ParseIdentifierReducer.class);
        job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class); 
        
	    FileInputFormat.addInputPath(job,new Path(inputPath));
        FileOutputFormat.setOutputPath(job,new Path(targetPath));         

        return job.waitForCompletion(true)?0:1;  
	}
	
	public static void main(String[] args)throws Exception{
        int exitcode = ToolRunner.run(new ParseIdentifierDriver(), args);
        System.exit(exitcode);                  
   }   
	
}

class ParseIdentifierMapper extends Mapper<LongWritable,Text,Text,Text>{
	public void map(LongWritable key,Text value,Context context) 
			throws IOException, InterruptedException{
		try
      {   
			String[] items = value.toString().split("\\|", -1);
			context.write(new Text(items[0]), new Text(items[1]));
		}
		catch (Exception e)  {;}
	}
}


class ParseIdentifierReducer  extends Reducer<Text,Text,Text,Text>{
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException     
	{
		Text v = new Text();
		String date = context.getConfiguration().get("date");
		
try {
	   EncryptUtil des = new EncryptUtil("aotain");
			
		for(Text t : values)
		{

			String items = t.toString().trim();
			items = items.replace("{", "").replace("}", "");
			String [] alls = items.split("],");
			for(String tp : alls){
				tp = tp.replace("\"]", "").replace("\"", "");
				String[] vss = tp.split(":\\[");
				String[] vsvalues = vss[1].contains(",") ? vss[1].split(","):new String[]{vss[1]};
				if(!"Phone".equals(vss[0])){
					for(String vsvalue : vsvalues){
						v.set(String.format("%s,%s,%s,%s,%s",key,vss[0],vsvalue,"1",date));	
						context.write(v, new Text(""));
					}
				}else if("Phone".equals(vss[0])){
					for(String vsvalue : vsvalues){
						vsvalue = des.decrypt(vsvalue) ;
						v.set(String.format("%s,%s,%s,%s,%s",key,vss[0],vsvalue,"1",date));	
						context.write(v, new Text(""));
					}
				}	
			}	
			
	 }
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}


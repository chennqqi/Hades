package com.aotain.project.gdtelecom;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.aotain.common.CommonFunction;

/**
 * 输入：Device2Check checked数据 + dim_devicecheck历史数据
 * 输出：dim_devicecheck数据
 * @author Administrator
 *
 */
public class DevicCheckDetail extends Configured implements Tool {
    public int run(String[] args) throws Exception {                  
		if( args.length != 3) {
			return 1;
		}
		
    	Configuration conf = new Configuration(); 
		
		String inputPaths = args[0];
		String targetPath = args[1];
		String date =  args[2];
		
		FileSystem fsTarget = FileSystem.get(URI.create(targetPath),conf);
        Path pathTarget = new Path(targetPath);
        if(fsTarget.exists(pathTarget))
        {
      	  	fsTarget.delete(pathTarget, true);
        }
		
        Job job = Job.getInstance(conf);
        job.setJobName("hdfs mr [DevicCheckDetail]" + date);                    
        job.setJarByClass(getClass());
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setReducerClass(DCDReducer.class);
        job.setNumReduceTasks(1);
        
	    String[] paths = inputPaths.split(",");
        for(int i = 0; i < paths.length; i++)
        {
        	MultipleInputs.addInputPath(job,new Path(paths[i]),TextInputFormat.class,DCDMapper.class);
        	System.out.println("----------------paths: "+paths[i]);
        }
        FileOutputFormat.setOutputPath(job,new Path(targetPath));         

        return job.waitForCompletion(true)?0:1;                  
    }

    public static void main(String[] args)throws Exception{
         int exitcode = ToolRunner.run(new DevicCheckDetail(), args);
         System.exit(exitcode);                  
    }   
}

class DCDMapper extends Mapper<LongWritable,Text,Text,Text> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		try{
			String kvs = value.toString() ;
			String splitdd ="@@@"; 
			String splitsh ="|";  
			
			if(kvs.contains(splitdd) && kvs.split(splitdd).length >=4 ){
				//new
				String[] items =kvs.split(splitdd);
				if(items[0] !=  null && !"".equals(items[0].trim())){
					context.write(new Text(items[0].trim()+splitsh+items[3].trim()), new Text("1|"+items[1].trim()+splitsh+items[2].trim()));
				}
			}else if(kvs.contains(splitsh) && kvs.split("\\"+splitsh).length >= 4 ){
				//bak
				String[] items =kvs.split("\\"+splitsh);
				if(items[0] !=  null && !"".equals(items[0].trim()) && !CommonFunction.isMessyCode(items[0])){
					context.write(new Text(items[0].trim()+splitsh+items[3].trim()), new Text("2|"+items[1].trim()+splitsh+items[2].trim()));
				}
			}
		
		}catch(Exception E){
			;
		}
	}
	
	
}

class DCDReducer  extends Reducer<Text,Text,Text,Text>{

	 public static Object getMinKey(Map<Integer, String> map) {
	        if (map == null) return null;
	        Set<Integer> set = map.keySet();
	        Object[] obj = set.toArray();
	        Arrays.sort(obj);
	        return obj[0];
	    }
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException     
	{
		StringBuffer sb = new StringBuffer();
		Map<Integer,String> newmap = new HashMap<Integer,String>();
		Map<Integer,String> bakmap = new HashMap<Integer,String>();
		String split ="|";  
		String keys = key.toString();
		String[] keyars = keys.split("\\"+split);
		String sub =new String();

		for(Text temp : values)
		{
			String[] arrs = temp.toString().split("\\"+split);
			if (arrs.length == 3) {
				sub = arrs[1].trim() + split + arrs[2].trim();
				if ("1".equals(arrs[0])) {
					newmap.put(sub.length(), sub);
				} else if ("2".equals(arrs[0])) {
					bakmap.put(sub.length(), sub);
				}
			}
		}
		
		if(bakmap.size() > 0){
			Object min = getMinKey(bakmap);
			sub = bakmap.get(min);
		}else if(newmap.size() > 0){
			Object min = getMinKey(newmap);
			sub = newmap.get(min);
		}
		
		if(sub != null && !"".equals(sub) 
			 &&	keyars[0] != null &&  !"".equals(keyars[0].trim())
			 && keyars[1].trim() != null  && !"".equals(keyars[1].trim())){
			sb.append(keyars[0].trim()).append(split)
			.append(sub).append(split)
			.append(keyars[1].trim()).append(split);
			context.write(new Text(sb.toString()), new Text(""));
		}
		
	}
	

}



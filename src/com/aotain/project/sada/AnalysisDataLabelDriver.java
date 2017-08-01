package com.aotain.project.sada;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



import com.hadoop.mapreduce.LzoTextInputFormat;



public class AnalysisDataLabelDriver extends Configured implements Tool {
	public int run(String[] args) throws Exception {                  
        System.out.println(args.length);
		if(args.length!=9)
			return 1;
		
    	Configuration conf = new Configuration(); 
		
		String[] inputPaths = args[0].split(",");
		String targetPath = args[1];
		String splitflag= args[2];
		String date = args[3];
		String choosedValue=args[4];
		String orccloums=args[5];
		String Classconfig=args[6];
		String searchWord=args[7];
		String kwPath=args[8];
		//������Ŀ¼�Ѵ��ڣ���Ҫɾ��
		FileSystem fsTarget = FileSystem.get(URI.create(targetPath),conf);
        Path pathTarget = new Path(targetPath);
        if(fsTarget.exists(pathTarget))
        {
      	  	fsTarget.delete(pathTarget, true);
        }
        StringBuffer kwstr = new StringBuffer();
		 FileSystem fsKws = FileSystem.get(URI.create(kwPath),conf); 
		FSDataInputStream in = null;
		try {
			Path path = new Path(kwPath);
			if (fsKws.exists(path)) {
				for (FileStatus file : fsKws.listStatus(path)) {
					in = fsKws.open(file.getPath());
					BufferedReader bis = new BufferedReader(
							new InputStreamReader(in, "UTF8"));
					String line = "";
					while ((line = bis.readLine()) != null) {
						kwstr.append(line.trim()).append("@@@");
					}
				}
			} else {
				System.out.println("not exist file !");
			}
		} finally {
			if (in != null)
				IOUtils.closeStream(in);
		}
		
		String kW = kwstr.toString();
		conf.set("kW", kW.substring(0, kW.length()-1));
	    String tableName = "datalabel";
	    conf.set("mapreduce.input.fileinputformat.input.dir.recursive","true");
	    conf.set("date", date);
	    conf.set("splitflag", splitflag);
	    conf.set("choosedValue", choosedValue);
		conf.set("orccloums", orccloums);
		conf.set("Classconfig", Classconfig);
		conf.set("searchWord", searchWord);
	
	    System.out.println("split str:"+splitflag);
	    System.out.println("choosedValue str:"+choosedValue);
	    System.out.println("classconfig :"+Classconfig);
	    System.out.println("kW :"+kW);
        Job job = Job.getInstance(conf);
        job.setJobName("hdfs mr [" + date + "], TableName:"+ tableName);                    
        job.setJarByClass(getClass());
        job.setMapperClass(AnalysisDataLabelMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setReducerClass(AnalysisDataLabelReducer.class);
        job.setNumReduceTasks(120);
        job.setMapOutputValueClass(Text.class);  
        //job.setOutputFormatClass(OrcNewOutputFormat.class);
        MultipleOutputs.addNamedOutput(job,"newHouse",TextOutputFormat.class,Text.class,IntWritable.class);  
        MultipleOutputs.addNamedOutput(job,"keyword",TextOutputFormat.class,Text.class,IntWritable.class);
        MultipleOutputs.addNamedOutput(job,"industry",TextOutputFormat.class,Text.class,IntWritable.class); 
        MultipleOutputs.addNamedOutput(job,"otherHouse",TextOutputFormat.class,Text.class,IntWritable.class); 
        MultipleOutputs.addNamedOutput(job,"type",TextOutputFormat.class,Text.class,IntWritable.class); 
        MultipleOutputs.addNamedOutput(job,"newCar",TextOutputFormat.class,Text.class,IntWritable.class); 
        MultipleOutputs.addNamedOutput(job,"specialbv",TextOutputFormat.class,Text.class,IntWritable.class); 
        MultipleOutputs.addNamedOutput(job,"specialbvct",TextOutputFormat.class,Text.class,IntWritable.class);
        MultipleOutputs.addNamedOutput(job,"carindustry",TextOutputFormat.class,Text.class,IntWritable.class);
        MultipleOutputs.addNamedOutput(job,"ershoufang",TextOutputFormat.class,Text.class,IntWritable.class);
        MultipleOutputs.addNamedOutput(job,"searchkeyword",TextOutputFormat.class,Text.class,IntWritable.class);
        MultipleOutputs.addNamedOutput(job,"yjqyInfo",TextOutputFormat.class,Text.class,IntWritable.class);
     
        job.setInputFormatClass(TextInputFormat.class);
      // job.setInputFormatClass(OrcNewInputFormat.class);
        for(int i=0;i<inputPaths.length;i++){
        	String[] p = inputPaths[i].split("#");
        	if(p[1].equals("lzo"))
        	{
        		MultipleInputs.addInputPath(job,new Path(p[0]),LzoTextInputFormat.class,AnalysisDataLabelMapper.class);
          	System.out.println("-------------lzopaths: "+p[0]);
        	}
        	else if(p[1].equals("orc"))
        	{
        		MultipleInputs.addInputPath(job,new Path(p[0]),OrcNewInputFormat.class,AnalysisDataLabelORCMapper.class);
        	
          	System.out.println("-------------orcpaths: "+p[0]);
       
        	}
        	else
        	{
        		MultipleInputs.addInputPath(job,new Path(p[0]),TextInputFormat.class,AnalysisDataLabelMapper.class);
          	System.out.println("-------------txtpaths: "+p[0]);
        	}
        }
       
        FileOutputFormat.setOutputPath(job,new Path(targetPath));         

        return job.waitForCompletion(true)?0:1;                  
    }

    public static void main(String[] args)throws Exception{
         int exitcode = ToolRunner.run(new AnalysisDataLabelDriver(), args);
         System.exit(exitcode);                  
    }   
}

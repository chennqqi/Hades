package com.aotain.ods;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.aotain.common.HFileConfigMgr;
import com.aotain.common.HfileConfig;
import com.aotain.common.HFileConfigMgr.FieldItem;
import com.hadoop.compression.lzo.LzopCodec;

public class PostDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {                  
		if(args.length!=4)
			return 1;
		
    	Configuration conf = new Configuration(); 
		
		String inputPath = args[0];
		String targetPath = args[1];
		String Config = args[2];
		String date = args[3];
		
		//如果输出目录已存在，需要删除
		FileSystem fsTarget = FileSystem.get(URI.create(targetPath),conf);
        Path pathTarget = new Path(targetPath);
        if(fsTarget.exists(pathTarget))
        {
      	  	fsTarget.delete(pathTarget, true);
        }
		
		HFileConfigMgr configMgr = null;
        
        configMgr = new HFileConfigMgr(Config);
		HfileConfig confHfile = configMgr.config;
		String tableName = confHfile.getTableName();
		
		String kvsplit = confHfile.getKVSplit()==null?"null":confHfile.getKVSplit();
		conf.set("kvsplit",kvsplit);
		System.out.println("-------------kvsplit: " + kvsplit);
		
		String fieldsplit = confHfile.getFieldSplit();
		conf.set("fieldsplit",fieldsplit);
		System.out.println("-------------fieldsplit: " + fieldsplit);
		
		String column = "";
		for(FieldItem item : confHfile.getColumns())
		{
			String text = String.format("%s=%s", item.FieldName,item.RegExp.length()>0?item.RegExp:"null");
			column += text +"#"; 
		}
		column  = column.substring(0,column.length() -1 );
		conf.set("column",column);
		System.out.println("-------------column: " + column);
		
        Job job = Job.getInstance(conf);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
        job.setJobName("hdfs mr [" + date + "], TableName:"+ tableName);                    
        job.setJarByClass(getClass());
        job.setMapperClass(PostMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setReducerClass(PostReducer.class);
        job.setMapOutputValueClass(Text.class);                  
        job.setInputFormatClass(TextInputFormat.class);
        
        FileInputFormat.addInputPath(job,new Path(inputPath));
        FileOutputFormat.setOutputPath(job,new Path(targetPath));         

        return job.waitForCompletion(true)?0:1;                  
    }

    public static void main(String[] args)throws Exception{
         int exitcode = ToolRunner.run(new PostDriver(), args);
         System.exit(exitcode);                  
    }   
}
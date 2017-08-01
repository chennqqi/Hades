package com.aotain.dw;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.aotain.common.HFileConfigMgr;
import com.aotain.common.HfileConfig;
import com.aotain.common.HFileConfigMgr.FieldItem;

/**
 * 疑似手机，邮箱数据提取
 * @author Administrator
 *
 */
public class PostSuspectDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length!=4)
		{
			System.err.printf("Usage: %s <PostPath><mobileAreaPath><targetPath><remark>",getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
    	Configuration conf = new Configuration(); 
		
		String inputPostPath = args[0];
		String mobileAreaPath = args[1];
		String targetPath = args[2];
		String remark = args[3];
		
		//如果输出目录已存在，需要删除
		FileSystem fsTarget = FileSystem.get(URI.create(targetPath),conf);
        Path pathTarget = new Path(targetPath);
        if(fsTarget.exists(pathTarget))
        {
      	  	fsTarget.delete(pathTarget, true);
        }
		
		
        Job job = Job.getInstance(conf);
        job.setJobName("Post Suspect[" + remark + "]");                    
        job.setJarByClass(getClass());
        
        job.setMapperClass(PostSuspectMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setReducerClass(PostSuspectReducer.class);
        //job.setInputFormatClass(TextInputFormat.class);
        MultipleOutputs.addNamedOutput(job,"MOBILE",TextOutputFormat.class,Text.class,Text.class);
        MultipleOutputs.addNamedOutput(job,"MAIL",TextOutputFormat.class,Text.class,Text.class);
        job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);  
        
        FileInputFormat.addInputPath(job,new Path(inputPostPath));
        FileInputFormat.addInputPath(job,new Path(mobileAreaPath));
        
        FileOutputFormat.setOutputPath(job,new Path(targetPath));         

        return job.waitForCompletion(true)?0:1;  
	}
	
	public static void main(String[] args)throws Exception{
        int exitcode = ToolRunner.run(new PostSuspectDriver(), args);
        System.exit(exitcode);                  
   }   

}

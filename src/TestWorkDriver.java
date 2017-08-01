

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.aotain.common.ObjectSerializer;
import com.hadoop.mapreduce.LzoTextInputFormat;


public class TestWorkDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {                  
		if(args.length!=3)
			return 1;
		
    	Configuration conf = new Configuration(); 
		
    	String sourcePath = args[0];
		String cachePath = args[1];
		String targetPath = args[2];
		
		FileSystem fsSource = FileSystem.get(URI.create(sourcePath),conf);
		Path pathSource = new Path(sourcePath);
	
		if(!fsSource.exists(pathSource))
		{
			return 1;
		}
		
	
		Map<String,String> map=new HashMap<String,String>();
		//如果输出目录已存在，需要删除
		FileSystem fsTarget = FileSystem.get(URI.create(targetPath),conf);
        Path pathTarget = new Path(targetPath);
        if(fsTarget.exists(pathTarget))
        {
      	  	fsTarget.delete(pathTarget, true);
        }
        
        FileSystem fs = FileSystem.get(URI.create(cachePath),conf); 
        int cnt = 0;
    	FSDataInputStream in = null; 
        try{ 
        	Path path = new Path(cachePath);
        	if(fs.exists(path))
        	{
        		for(FileStatus file:fs.listStatus(path))
				{
        			in = fs.open(file.getPath());
        			BufferedReader bis = new BufferedReader(new InputStreamReader(in,"UTF8")); 
        			String line = "";
        			while ((line = bis.readLine()) != null) { 
        				String[] arr = line.split(",",-1);
        				map.put(arr[1], arr[0]);
        				cnt++;
        				}
				}
        	}
        	else
        	{
        		System.out.println("not exist file !"); 
        	}
        }finally{
        	if(in != null)
        		IOUtils.closeStream(in); 
        } 
        conf.set("map",
                ObjectSerializer.serialize((Serializable) map));
		System.out.println("-------------size: " + cnt);
		
        
        Job job = Job.getInstance(conf);
        job.setJobName("Test Conf Collections");                    
        job.setJarByClass(getClass());
        job.setMapperClass(TestWorkMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setReducerClass(TestWorkReducer.class);
        job.setMapOutputValueClass(Text.class);
//        job.setInputFormatClass(LzoTextInputFormat.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job,new Path(sourcePath)); 
        FileOutputFormat.setOutputPath(job,new Path(targetPath));

        
        return job.waitForCompletion(true)?0:1;                  
    }

    public static void main(String[] args)throws Exception{
         int exitcode = ToolRunner.run(new TestWorkDriver(), args);
         System.exit(exitcode);                  
    }   
}
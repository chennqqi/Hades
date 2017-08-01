
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxTemperatureDriver extends Configured implements Tool {
    @SuppressWarnings("deprecation")

    public int run(String[] args) throws Exception {                  

    	
        
              if (args.length != 2){

                       System.err.printf("Usage: %s <input><output>",getClass().getSimpleName());

                       ToolRunner.printGenericCommandUsage(System.err);

                       return -1;                  

              }                  

              //Test("/user/hive/warehouse/");
              //Configuration conf =getConf();                
              
              Job job = Job.getInstance(new Configuration());

              job.setJobName("Max Temperature");                  

              job.setJarByClass(getClass());

              FileInputFormat.addInputPath(job,new Path(args[0]));

              FileOutputFormat.setOutputPath(job,new Path(args[1]));
              
              job.setNumReduceTasks(1);

              job.setMapperClass(MaxTemperatureMapper.class);

              job.setReducerClass(MaxTemperatureReducer.class);                  

              job.setOutputKeyClass(Text.class);

              job.setOutputValueClass(IntWritable.class);                  

              return job.waitForCompletion(true)?0:1;                  

    }


    public static void main(String[] args)throws Exception{
         int exitcode = ToolRunner.run(new MaxTemperatureDriver(), args);
         System.exit(exitcode);                  
    }   
    
    
    private void Test(String arg0) throws Exception
    {
    	  //第一个参数传递进来的是hadoop文件系统中的某个文件的URI,以hdfs://ip 的theme开头 
        String uri = arg0; 
        //读取hadoop文件系统的配置 
        Configuration conf = new Configuration(); 
        conf.set("hadoop.job.ugi", "hadoop-user,hadoop-user"); 
         
        //FileSystem是用户操作HDFS的核心类，它获得URI对应的HDFS文件系统 
        FileSystem fs = FileSystem.get(URI.create(uri),conf); 
        FSDataInputStream in = null; 
        try{ 
        	
        	Path path = new Path(uri);
        	if(fs.exists(path))
        	{
        		System.out.println(uri); 
        		System.out.println("exist file !!!!!!"); 
        	}
        	else
        	{
        		System.out.println(uri); 
        		System.out.println("not exist file !!!!!!"); 
        	}
        }finally{ 
            IOUtils.closeStream(in); 
        }
    }
}

package dmpcommon;

import java.io.Serializable;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.aotain.common.HFileConfigMgr;
import com.aotain.common.HfileConfig;
import com.aotain.common.ObjectSerializer;
import com.aotain.common.HFileConfigMgr.FieldItem;

public class KVTWDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {                  
		if(args.length!=4)
			return 1;
		
    	Configuration conf = getConf(); 
		
		String inputPath = args[0];
		String targetPath = args[1];
		String Config = args[2];
		String date = args[3];
		

		FileSystem fsTarget = FileSystem.get(URI.create(targetPath),conf);
        Path pathTarget = new Path(targetPath);
        if(fsTarget.exists(pathTarget))
        {
      	  	fsTarget.delete(pathTarget, true);
        }
        
		conf.set("date",date);
		
		HFileConfigMgr configMgr = new HFileConfigMgr(Config);
		HfileConfig confHfile = configMgr.config;
		
		String tableName = confHfile.getTableName();
		System.out.println("-------------fieldsplit: " + tableName);

		String fieldsplit = confHfile.getFieldSplit();
		conf.set("fieldsplit",fieldsplit);
		System.out.println("-------------fieldsplit: " + fieldsplit);

		Map<String,Integer> mapPeriod = new HashMap<String,Integer>();
		String column = "";
		for (FieldItem item : confHfile.getColumns()) {
			mapPeriod.put(item.FieldName, item.Period);
			String text = String.format("%s=%d=",item.FieldName, item.Period);
			column += text + "#";
		}
		conf.set("KvPeriod", ObjectSerializer.serialize((Serializable) mapPeriod));
		column = column.substring(0, column.length() - 1);
		System.out.println("-------------column: " + column);

        Job job = Job.getInstance(conf);
        job.setJobName("hdfs mr [" + date + "], TableName:"+ tableName);                    
        job.setJarByClass(getClass());
        job.setMapOutputKeyClass(Text.class);
        job.setReducerClass(KVTWReducer.class);
        job.setMapOutputValueClass(Text.class);    

        String[] paths = inputPath.split("#");
        for(int i=0; i<paths.length; i++)
        {
        	MultipleInputs.addInputPath(job,new Path(paths[i]),TextInputFormat.class,KVTWMapper.class);
        	System.out.println("-------------paths: " + paths[i]);
        }
        
        FileOutputFormat.setOutputPath(job,new Path(targetPath));         

        return job.waitForCompletion(true)?0:1;                  
    }

    public static void main(String[] args)throws Exception{
         int exitcode = ToolRunner.run(new KVTWDriver(), args);
         System.exit(exitcode);                  
    }   
}
package com.aotain.project.gdtelecom.identifier.test;

import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;  
  
public class TestArgs extends Configured implements Tool {  
    @Override  
    public int run(String[] args) throws Exception {  
    	
        Configuration conf = getConf();  // 不能使用new Configuration();
        for (Entry<String, String> entry : conf) {  
            System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());  
        } 
        // 其它参数
        for(String arg : args) {
        	System.out.println("other args:" + arg);
        }
        return 0;  
        
    }  
  
    public static void main(String[] args) throws Exception {  
        int exitCode = ToolRunner.run(new TestArgs(), args);  
        System.exit(exitCode);  
    }  
}  
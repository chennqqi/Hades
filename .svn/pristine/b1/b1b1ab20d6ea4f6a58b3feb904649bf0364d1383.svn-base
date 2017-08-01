
import java.io.IOException; 

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

 


public class MaxTemperatureMapper extends Mapper<LongWritable, Text,Text, IntWritable>{
	  @Override
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{                                    

		  
		  	
                String line =value.toString();                               
                try {

                		String[] values = line.split(",",-1);
                        String year =values[0];

                        for(int i = 1;i<values.length;i++)
                        {
                        	int airTemperature1 = Integer.parseInt(values[i]);            
                        //int airTemperature2 = Integer.parseInt(values[2]);

                       		context.write(new Text(year),new IntWritable(airTemperature1));                           
                        //context.write(new Text(year+"-2"),new IntWritable(airTemperature2));
                        }
                        
                        

                } catch (Exception e) {

                         System.out.print("Error in line:" + line);

                }                                  

      }        
	  
}

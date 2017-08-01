


import java.io.IOException;




import java.util.HashMap;



import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aotain.common.ObjectSerializer;


public class TestWorkMapper extends Mapper<LongWritable, Text, Text, Text> {

	 private static HashMap<String,String> map=null;
	
	  @SuppressWarnings("unchecked")
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		  String str = context.getConfiguration().get("map");
		  map  = ( HashMap<String,String>) ObjectSerializer
		            .deserialize(str);
	}

	@Override
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{                         

		  String fieldsplit = "|";//列属性分隔符
		  String[] items;	  
    
		  
		  
		  try
		  {
			  //源文件据处理
			  items = value.toString().split("\\"+fieldsplit,-1);
			
			  if(items[0].length()>0)
			  {
				  if(map.get(items[2].trim()) != null){
					  context.write(value,new Text(""));
				  }
			  }
		 }
     	 catch(Exception e)
     	 {;}
      }


}
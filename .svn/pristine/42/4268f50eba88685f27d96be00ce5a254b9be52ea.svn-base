import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class TestMapperForFile extends TableMapper<Text, IntWritable> {

	@SuppressWarnings("deprecation")
	public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException, InterruptedException{ 
		String bsc = "";
	    String accesscell = "";
		 for (KeyValue c : values.list()) {
	      {	      
	      //bsc
	    	  if(Bytes.toString(c.getQualifier()).equals("BSC"))
	    	  {
	    		  bsc = Bytes.toString(c.getValue());
	    	  }
	    	  else if (Bytes.toString(c.getQualifier()).equals("ACCESS_CELL"))
	    	  {
	    		  accesscell = Bytes.toString(c.getValue());
	    	  }
	    	 
	      }
	      String skey = bsc + "_" + accesscell;
    	  
	      //ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes(skey));
	      //ImmutableBytesWritable value = new ImmutableBytesWritable(Bytes.toBytes(1));
		  try {
				context.write(new Text(skey),new IntWritable(1));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	      
	   }
	}
}

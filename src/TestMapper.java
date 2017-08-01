import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;


public class TestMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {

	 @Override
	 public void map(ImmutableBytesWritable row, Result values,Context context) throws IOException {
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
    	  
	      ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes(skey));
	      ImmutableBytesWritable value = new ImmutableBytesWritable(Bytes.toBytes(1));
		  try {
				context.write(key,value);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	      
	   }
	 }

}

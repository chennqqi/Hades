import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class TestReducer extends  TableReducer <ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

	 @Override
	 public void reduce(ImmutableBytesWritable key,Iterable<ImmutableBytesWritable> values,
			   Context context) throws IOException, InterruptedException {
		  int count = 0;
		  for (ImmutableBytesWritable val : values) {
			  count= count +
					  Bytes.toInt(val.get());
		  }
		  Put put = new Put(key.get());
		  put.add(Bytes.toBytes("info"), 
				  Bytes.toBytes("bsc_accesscell"),
				  Bytes.toBytes(String.valueOf(count)));
		  
		  context.write(key,put);
	 }
	 
}

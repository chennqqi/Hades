import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class TestReducerForFile extends Reducer<Text,IntWritable,Text,IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException        {

		int count = 0;
		  for (IntWritable val : values) {
			  count= count + val.get();
		  }
		 
		//String sKey = Bytes.toString(key.get());
		
        context.write(key, new IntWritable(count));
	} 
	
}

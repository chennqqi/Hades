import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

  
public class MRTest {

	public static void main(String[] args) throws Exception {
		String startkey =  args[0];
		String endkey = args[1];
		
		
		
		 Configuration conf = new Configuration();
		 conf = HBaseConfiguration.create(conf);
		 Job job = Job.getInstance(conf, "HBase_BSC_File");
		 job.setJarByClass(MRTest.class);
		 FileOutputFormat.setOutputPath(job,new Path(args[2]));
		 
		 Scan scan = new Scan(startkey.getBytes(), endkey.getBytes());
		 scan.addColumn(Bytes.toBytes("info"),Bytes.toBytes("BSC"));
		 scan.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ACCESS_CELL"));
		 
		 //TableMapReduceUtil.initTableMapperJob("cdr_hw_1x", scan,TestMapper.class,
		 //ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);
		 //TableMapReduceUtil.initTableReducerJob("bsccellcount",TestReducer.class, job);
		 
		 
		 TableMapReduceUtil.initTableMapperJob("cdr_hw_1x", 
				 scan,TestMapperForFile.class,
				 Text.class, IntWritable.class, job);
				 
		 
		 job.setReducerClass(TestReducerForFile.class);
		 //job.setOutputKeyClass(ImmutableBytesWritable.class);
         //job.setOutputValueClass(Put.class);   
         
		 
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}

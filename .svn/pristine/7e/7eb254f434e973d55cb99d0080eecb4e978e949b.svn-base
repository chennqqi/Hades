package com.aotain.ods;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.mapreduce.LzoTextInputFormat;

public class IDCDataMerger {

	public static void main(String[] args) {
		   //System.out.println("0###OK################################");
		
		if (args.length != 2){
         System.err.printf("Usage: %s <input><output>");
         System.exit(1);
		}          
		
		//HTTPDCSpark spark = new HTTPDCSpark();
		
		SparkFunc(args);
		   //System.out.println("1###OK################################");
	}
	
	 public static int SparkFunc(final String[] args){
		 try
		 {
			 
		    SparkConf sparkConf = new SparkConf().setAppName("Data Merger Spark");
		    Configuration config = new Configuration();
		    config.addResource("/etc/hadoop/conf");
		    
		    System.out.println("#%%%HDFS:" + config.get("fs.defaultFS"));
		    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		    
		    
		    String sourcePath = args[0];
		    System.out.println("sourcePath: " + sourcePath);
		    //FileSystem fsSource = FileSystem.get(URI.create(sourcePath),config);
		    //Path pathSource = new Path(sourcePath);
		    //if(!fsSource.exists(pathSource))
		   // {
		    //	return 0;
		    //}
		    
		    //目标目录维护
		    String targetPath = args[1];
		    System.out.println("targetPath: " + targetPath);
		    FileSystem fsTarget = FileSystem.get(URI.create(targetPath),config);
		    Path pathTarget = new Path(targetPath);
		    if(fsTarget.exists(pathTarget))
		    {
		    	fsTarget.delete(pathTarget, true);
		    	System.out.println("Delete path " + targetPath);
		    }
		    
		    
		    
		    //JavaRDD<String> lines = ctx.textFile(ip, 1);
		    JavaPairRDD<LongWritable,Text> tt=ctx.newAPIHadoopFile(sourcePath,LzoTextInputFormat.class,  LongWritable.class, Text.class,config);
		    
		   // tt.mapToPair(f)
		    JavaPairRDD<String, String> s = tt.mapToPair(new PairFunction<Tuple2<LongWritable,Text>, String,String>() {
		    	/**
				 * 
				 */
				private static final long serialVersionUID = 1L;
				@Override
				public Tuple2<String, String> call(Tuple2<LongWritable, Text> v1) throws Exception {
					
					String row = v1._2.toString() + "|";
					return new Tuple2<String, String>(row , "");
				}
			}).repartition(128);
			
		    s.saveAsHadoopFile(targetPath, Text.class, Text.class, TextOutputFormat.class, LzopCodec.class);
		    
		    ctx.stop();
		    return 0;
		 }
		 catch(Exception ex)
		 {
			 ex.printStackTrace();
			 return 1;
		 }
		 
		   
	   }
}

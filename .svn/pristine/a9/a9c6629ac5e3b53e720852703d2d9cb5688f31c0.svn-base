package com.aotain.spark;

import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.aotain.common.CommonFunction;
import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.mapreduce.LzoTextInputFormat;

public class HTTPDCCheck {

	public static void main(String[] args) {
		   //System.out.println("0###OK################################");
		
		if (args.length != 2){
         System.err.printf("Usage: %s <input><output>");
         System.exit(1);
		}          
		
		//HTTPDCSpark spark = new HTTPDCSpark();
		int nexit = HTTPDCCheck(args);
		   //System.out.println("1###OK################################");
	}
	
	public static int HTTPDCCheck(final String[] args){
		try
		 {
		    SparkConf sparkConf = new SparkConf().setAppName("HTTP DC Check");
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
					
					String row = v1._2.toString();
					String items[]  = row.split("\\|",-1);
					//String result = "";
					if(items.length != 17)
					{
						Tuple2<String, String> result = new Tuple2<String, String>(row, "");
						return result;
					}
					return new Tuple2<String, String>("", "");
					 
				}
			}).reduceByKey(new Function2<String,String,String>(){

				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override
				public String call(String v1, String v2) throws Exception {
					// TODO Auto-generated method stub
					if(v1.isEmpty())
					{
						return "";
					}
					return v1;
				}},1);
	       
		    //s.repartition(256);
		    //s.saveAsTextFile(targetPath, LzopCodec.class);
		    s.saveAsHadoopFile(targetPath, Text.class, Text.class, TextOutputFormat.class);
		    //s.saveAsNewAPIHadoopFile(targetPath,Text.class,Text.class,LzoTextInputFormat.class,config);
		    
		    //System.out.println("2###OK################################"+s.count());
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

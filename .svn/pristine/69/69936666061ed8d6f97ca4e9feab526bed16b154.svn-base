package com.aotain.project.dmp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.mapreduce.LzoTextInputFormat;
import com.hadoop.mapreduce.LzoIndexOutputFormat;


public class PraseCookie  extends Configured implements Tool {

	private final static String cut = "|";
	private static final Log log = LogFactory.getLog(PraseCookie.class);
	private static Configuration conf = null;
	
	
	public static class UserMDFMapper extends Mapper<LongWritable, Text, Text, Text> {	
	
	private Map<String, String> regexMap = new HashMap<String,String>();	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		 
			try{
				String partdate = context.getConfiguration().get("partdate");
				String line = value.toString();
				String outkey = "";
				String items[] = line.split("\\|", -1);
				String cookie = "";
				String domain = "";
				String username = "";
				String lastdate = "";

				if (items.length > 11) {
					cookie = items[12];
					domain = items[3];
					username = items[1];
					lastdate = items[11].substring(0, 8);
					String domains[] = domain.split("\\.");
					domain = domains[domains.length - 2] + "."
							+ domains[domains.length - 1];
					String rules = regexMap.get(domain);
					
					    if(rules!=null){
					    	String regex = rules.split("\\|")[0];
					    	cookie = findMathc(cookie, regex);
					    	}
						
					    else{
					    	cookie="";
					    	}

				} else if (items.length == 5) {
					cookie = items[2];
					domain = items[1];
					username = items[0];
					lastdate = items[3];

				}
				
				SimpleDateFormat dfTime = new SimpleDateFormat("yyyyMMdd");
				Date dAccessTime = dfTime.parse(lastdate);

				Date cDate = dfTime.parse(partdate);

				outkey = username + "|" + domain + "|" + cookie;
				String rules = regexMap.get(domain);
				if(rules!=null){
					int outdate = Integer.parseInt(rules.split("\\|")[1]);
					long ctime = (cDate.getTime() - dAccessTime.getTime()) / 1000 / 60 / 60/24;
					if (ctime > outdate) {
						lastdate = "";
					}
				}else{
					lastdate = "";
				}
				outkey = username + "|" + domain + "|" + cookie;
				if (!"".equals(lastdate) && !"".equals(cookie))
					context.write(new Text(outkey), new Text(lastdate));

			 }catch(Exception e){
				 
			 }
		
		}
	
	 @Override
	  public void setup(Context context) throws IOException,
	  InterruptedException {
   	  conf = context.getConfiguration();
   	 parseFile("/user/project/dmp/regexConfig/regext.txt");
   	 
   	  
   	  
   	  
  }	
		private static String findMathc(String url,String regex){
			 Pattern p = Pattern.compile(regex,Pattern.CASE_INSENSITIVE);
		        Matcher m = p.matcher(url); 
		        String match="";
		      
		        while (m.find()) {
		        	match=m.group(1);
		        } 
		        return match;
		}
	 private void parseFile(String fileName) throws IOException {
		  FileSystem fs = FileSystem.get(URI.create(fileName),conf); 
		  FSDataInputStream in = null; 
		  BufferedReader fis=null;
	      try {
	    	Path mobilePath = new Path(fileName);
	    	in = fs.open(mobilePath);
	        fis = new BufferedReader(new InputStreamReader(in,"GBK"));
//	    	fis = new BufferedReader(new FileReader(fileName));
	        String pattern = null;
//	        int i = 0;
	        String[] cols=null;
	        while ((pattern = fis.readLine()) != null) {
	        	 cols = pattern.split(",");
	        	 regexMap.put(cols[0], cols[1]);
	     		
	        }
	      } catch (IOException ioe) {
	        System.err.println("Caught exception while parsing the  file '");
	      }finally{
	    	  if(in != null){
	    		  in.close();
	    	  }
	    	  if(fis != null){
	    		  fis.close();
	    	  }
	      }
	    }
	}
	
	
	public static class UserMDFReducer extends Reducer<Text, Text, Text, Text> {		
		protected void reduce(Text key,  Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
			int big=0;
			 for(Text value:values){
				int count=Integer.parseInt(value.toString().trim());
				if(big<count){
					big=count;
				}
					}
			
			 String skey=key.toString();
			 context.write(new Text(skey+"|"+big+"|"),new Text(""));
			
		}
	}

	
	/** 初始化JOB
	 * @param
	 * args[0]=domain  args[1]=outputpath args[2]=inputpath
	 * jarname usermd5 /user/data/usermdf/out/20151201  /user/hive/warehouse/broadband.db/to_opr_http/shenzhen/20151201
	 */
	public Job UserMDFJob(final Configuration conf,final String[] args) throws IOException {		
		
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");		
		String[] statstamp = args[2].split("/");
		System.out.println();
		String jobname = ">>>crawler_data>>> "+args[0] + ">>>" + statstamp[statstamp.length-1];		
		String input = args[2];
		Job job = Job.getInstance(conf);
		job.setJobName(jobname);
		job.setJarByClass(PraseCookie.class);
		for (String pt : input.split(",")) {
			FileInputFormat.addInputPath(job,new Path(pt));
		}
		
		job.setMapperClass(UserMDFMapper.class);
		Path outputpath = new Path(args[1]);
		outputpath.getFileSystem(conf).delete(outputpath, true);
		FileOutputFormat.setOutputPath(job, outputpath);
		//job.setInputFormatClass(TextInputFormat.class);
		job.setInputFormatClass(LzoTextInputFormat.class);		
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
		
		job.setReducerClass(UserMDFReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return job;
	}
	
	
	/**
	 * @param errorMsg Error message. Can be null.
	 */
	private static void usage(final String errorMsg) {
		if (errorMsg != null && errorMsg.length() > 0) {
			log.error("ERROR: " + errorMsg);
		}
		log.info("Usage: hadoop jar XX.jar jobname output inputlist");

	}
	
	public static void main(final String[] args) throws Exception {
		log.info("-------UserMDF main start----");
		PropertyConfigurator.configure("../conf/log4j.properties");
		int exitCode = ToolRunner.run(new PraseCookie(), args);
		
		System.exit(exitCode);
	}
	
	/** 
	 * @param
	 * args[0]=jobname  args[1]=outputpath args[2]=inputpath args[3]=impalanode args[4]=province 
	 */
	public int run(String[] args) throws Exception {
		conf = new Configuration();	
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		for(int i=0;i<otherArgs.length;i++){
			log.info(otherArgs[i]);
		}
		if (otherArgs.length != 5) {
			usage("Wrong number of arguments: " + otherArgs.length);
			System.exit(-1);
		}		
		String city=otherArgs[3];
		String partdate=otherArgs[4];
		conf.set("city", city);
		conf.set("partdate", partdate);
		Job statics = UserMDFJob(conf,otherArgs);
		int ret = statics.waitForCompletion(true) ? 0 : 1;		
		return ret;
		

		
	}
}




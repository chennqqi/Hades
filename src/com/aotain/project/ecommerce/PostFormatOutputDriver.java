package com.aotain.project.ecommerce;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.aotain.project.ecommerce.pojo.Post;
import com.aotain.project.ecommerce.rule.Rule;
import com.aotain.project.ecommerce.rule.RuleManager;

public class PostFormatOutputDriver  extends Configured implements Tool {

	private static final Log log = LogFactory.getLog(PostFormatOutputDriver.class);
	
	
	public static class PostFormatOutputMapper  extends Mapper<LongWritable,Text,Text,Text> {
	
	private  Configuration conf = null;
	private  RuleManager ruleManager = new RuleManager();
	private List<String> neededDomains;
	
	private String[] columns = {"UserName","Phone","Price","Mail","CityCode","DetailAddress","NickName","Passwd","PostCode","PostPrice","ProductsName","PaymentMethod","Sex","RequireDeliveDate","Telephone","DepositRate","ProductsInfo","VehicleNumber","FrameNumber","VehicleType","EngineNumber","CodedFormat","Birthday","Areacode","QQAccount"};
	private static final String SEP = "|";
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		conf= context.getConfiguration();
		String[] domains = {"jd.com", "taobao.com", "tmall.com", "chiphell.com", "kaola.com", "gome.com.cn", "suning.com"};
		
		String post_rule = conf.get("post_rule_conf");
		String post_person = conf.get("post_rule_person");
		FileSystem post_rule_fs = FileSystem.get(URI.create(post_rule), conf);
		FileSystem post_person_fs = FileSystem.get(URI.create(post_person), conf);
		ruleManager.loadConfRule(post_rule_fs.open(new Path(post_rule)), post_person_fs.open(new Path(post_person)), domains, null);
		
/*		ruleManager.loadConfRule(new FileInputStream("E:/work/dev/数据提取 ecommerce/post/post_rule.xml"),
				new FileInputStream("E:/work/dev/数据提取 ecommerce/post/post_person.xml"), domains, null);
		System.out.println(ruleManager.printRule());*/
		
		neededDomains = grepDomain(domains);
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] datas = value.toString().split("\\|");
		if(datas.length < 14) {
			return;
		}
		Post post = new Post(datas);
		
		StringBuffer sb = new StringBuffer();
		Boolean write = false;
		
		for(String domain : neededDomains) {
			if(post.getHost().equals(domain)) {
				sb.append(post.getUsername()).append(SEP).append(SEP)
					.append(post.getHost()).append(SEP)
					.append(post.getUrl()).append(SEP).append(SEP).append(SEP).append(SEP)
					.append(post.getSrcip()).append(SEP)
					.append(post.getDestip()).append(SEP)
					.append(post.getCreatetime()).append(SEP);
					
				for(int i=0,len=columns.length; i<len; i++) {
					String col = columns[i];
					Rule rule = ruleManager.getRule(post.getHost());
					String columnVal = "";
					if(null != rule) {
						String dataCol = rule.getPairKFromPairV(col);
						if (null != dataCol) {
							columnVal = findMathc(post.getPostcont(), "(&|^)" + dataCol  + "=(.*?)(&|$)");
							if(!columnVal.equals("")) {
								write=true;
								columnVal = CodeUtils.decode(columnVal);
							}
						}
					}
					sb.append(columnVal).append(SEP);
				}
				if(write) {
					context.write(new Text(sb.substring(0, sb.length()-1)), new Text(""));
//					System.out.println(sb.substring(0, sb.length()-1));
				}
				break;
			}
		}
	}
	
	/*
	 * 通过domains，过滤出配置文件中有的host
	 */
	private List<String> grepDomain(String[] domains) {
		List<String> resultList = new ArrayList<String>();
		for(String host : ruleManager.getAllHost()) {
			for(String domain : domains) {
				if(host.contains(domain)) {
					resultList.add(host);
				}
			}
		}
		return resultList;
	}
	
	/**
	 * 按规则查找
	 * @param url  如mobile=13421812993&id_md5=359a3d10248653da268661947a7cece
	 * @param regex  如 mobile=(.*)&
	 * @return 如 13421812993
	 */
	public static String findMathc(String url, String regex) {
		Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(url);
		String match = "";
		int i=0;
		while (m.find()) {
			if(i>=1) {
				System.out.println(regex);
				System.out.println(match);
				match +="#";
			}
			i++;
			match = match + m.group(2);
		}
		return match;
	}

}

	public static class PostFormatInfoReducer extends Reducer<Text,Text,Text,Text>{
		
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException        {
	      	  context.write(key, new Text(""));  
		}
	}

	
	/** 初始化JOB
	 * @param
	 */
	public Job initJob(final Configuration conf,final String[] args) throws IOException {		
		
		String input = args[1];
		Job job = Job.getInstance(conf);
		job.setJobName("Post Format");
		job.setJarByClass(PostFormatOutputDriver.class);
		for (String pt : input.split(",")) {
			FileInputFormat.addInputPath(job,new Path(pt));
		}
		
		job.setMapperClass(PostFormatOutputMapper.class);
		Path outputpath = new Path(args[0]);
		outputpath.getFileSystem(conf).delete(outputpath, true);
		FileOutputFormat.setOutputPath(job, outputpath);
		job.setInputFormatClass(TextInputFormat.class);
		//job.setInputFormatClass(OrcNewInputFormat.class);		
		//FileOutputFormat.setCompressOutput(job, true);
		
		job.setReducerClass(PostFormatInfoReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		return job;
	}
	
	
	/**
	 * @param errorMsg Error message. Can be null.
	 */
	private static void usage(final String errorMsg) {
		if (errorMsg != null && errorMsg.length() > 0) {
			log.error("ERROR: " + errorMsg);
		}
		log.info("Usage: hadoop jar ecommerce.jar com.aotain.project.ecommerce.PostFormatOutputDriver  <out> <in> <post_rule.xml> <post_person.xml>");

	}
	
	public static void main(final String[] args) throws Exception {
		log.info("-------UserMDF main start----");
		int exitCode = ToolRunner.run(new PostFormatOutputDriver(), args);
		System.exit(exitCode);
	}
	
	/** 
	 * @param
	 *  args[0]=outputpath args[1]=inputpath args[2]=post_rule.xml args[3]=post_person.xml
	 */
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();	
		conf.set("mapreduce.input.fileinputformat.input.dir.recursive","true");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		for(int i=0;i<otherArgs.length;i++){
			log.info(otherArgs[i]);
		}
		if (otherArgs.length != 4) {
			usage("Wrong number of arguments: " + otherArgs.length);
			System.exit(-1);
		}
		
		conf.set("post_rule_conf", otherArgs[2]);
		conf.set("post_rule_person", otherArgs[3]);
		
		Job statics = initJob(conf,otherArgs);
		int ret = statics.waitForCompletion(true) ? 0 : 1;		
		return ret;
		
	}
}


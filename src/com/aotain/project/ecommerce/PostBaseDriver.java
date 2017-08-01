package com.aotain.project.ecommerce;

import java.io.IOException;
import java.net.URI;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.aotain.project.ecommerce.pojo.Pair;
import com.aotain.project.ecommerce.rule.Rule;
import com.aotain.project.ecommerce.rule.RuleManager;

/**
 * 数据提取：根据规则，输出postcont字段里指定的属性及属性值
 * 数据源：to_opr_postorig表
 * 如postcont字段值 mobile=13421812993&user_name=ace，根据规则可能输出 Phone=User13421812993,Name=ace
 * 不包含规则的数据过滤
 * @author Ace
 *
 */
public class PostBaseDriver  extends Configured implements Tool {

	private static final Log log = LogFactory.getLog(PostBaseDriver.class);
	
	public static class UserMDFMapper extends Mapper<LongWritable, Text, Text, Text> {
		private RuleManager ruleManager = new RuleManager();
		private static Configuration conf = null;
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] items = value.toString().split("\\|");
			String[] cloums = { "BroadbandAccount=0", "WebSiteName=1", "Domain=6", "Url=7", "URLNo=1", "ClassID=1",
					"else=1", "SrcIP=2", "DstIP=3", "Time=11"};

			if(items.length < 14) {
				return;
			}
			
			String domain = items[6];
			String postcon = items[13];

			
			String replacedCol = findAndReplaceColumn(postcon, domain);
			if (replacedCol.equals("")) {
				return;
			}
			
			StringBuffer outkey = new StringBuffer();
			for (int i = 0; i < cloums.length; i++) {
				int index = Integer.parseInt(cloums[i].split("=")[1]);
				if (index != 1)
					outkey.append(",").append(cloums[i].replace(index + "", items[index]));
				else
					outkey.append(",").append(cloums[i].replace(index + "", ""));
			}
			String citycode = conf.get("citycode");
			String partdate = conf.get("partdate");
			outkey.append(",").append(replacedCol).append(citycode).append(",").append(partdate);
			context.write(new Text(domain), new Text(outkey.substring(1, outkey.length())));
//			System.out.println(value);
//			System.out.println(new Text(outkey.substring(1, outkey.length())));

		}

		private String findAndReplaceColumn(String postcon, String host) {
			String result = "";
			Rule rule = ruleManager.getRule(host);
			if(null != rule) {
				List<Pair> rulePairs = rule.getColumnPair();
				for(Pair pair : rulePairs) {
					String needRule = "(&|^)" + pair.getKey() + "=(.*?)(&|$)";// 需要查找的正则表达式 
					needRule = findMathc(postcon, needRule);
					if (needRule == null || "".equals(needRule.trim())) {
						continue;
					}
					
					//解码
					needRule = CodeUtils.decode(needRule);
					// 部分数据需要解码两次
					needRule = CodeUtils.decode(needRule);
					
					String useCloum = pair.getValue();
					String lastCloum = useCloum + "=" + needRule;
					result = lastCloum+","+result;
				}
			}
			return result;
		}
		

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);

			conf = context.getConfiguration();

//			loadConfRule();
			String post_rule = conf.get("post_rule_conf");
			String post_person = conf.get("post_rule_person");
			FileSystem post_rule_fs = FileSystem.get(URI.create(post_rule), conf);
			FileSystem post_person_fs = FileSystem.get(URI.create(post_person), conf);
			
			// 只装载这部分domain
			String[] domains = {"www.guazi.com","www.renrenche.com","www.kaola.com","www.fengqu.com",
					"gz.jz.jiehun.com.cn","www.homekoo.com",
					"www.aijaw.com","www.yigaojiaju.com",
					"www.niernuojiaju.com","www.ebery.com.cn",
					"www.artist88.com","www.easily-china.com",
					"www.wy100.com","www.suofeiya.com.cn",
					"eastmoney.com","pay.gw.com.cn",
					"i.10jqka.com.cn","pass.10jqka.com.cn",
					"search.xcar.com.cn","newcar.xcar.com.cn",
					"www.rong360.com","www.huirendai.com",
					"www.yonglibao.com","www.jiuxindai.com",
					"www.weimob.com"};
			ruleManager.loadConfRule(post_rule_fs.open(new Path(post_rule)), post_person_fs.open(new Path(post_person)), domains, null);

//			ruleManager.loadConfRule(new FileInputStream("E:/work/dev/1.省电渠数据提取 ecommerce/post/post_rule.xml"),
//					new FileInputStream("E:/work/dev/1.省电渠数据提取 ecommerce/post/post_person.xml"), domains, null);
			
		}

	 /**
	  * 按规则查找
	  * @param url  如mobile=13421812993&id_md5=359a3d10248653da268661947a7cece
	  * @param regex 如 mobile=(.*)&
	  * @return  如 13421812993
	  */
		private String findMathc(String url,String regex){
			 Pattern p = Pattern.compile(regex,Pattern.CASE_INSENSITIVE);
		        Matcher m = p.matcher(url); 
		        String match="";
		      
		        if (m.find()) {
		        	match=m.group(2);
		        } 
		        return match;
		}
		
	}
	
	
	public static class UserMDFReducer extends Reducer<Text, Text, Text, Text> {	
		
		private MultipleOutputs<Text, Text> output;
		
		protected void reduce(Text key,  Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
			for(Text value : values) {
				String domain = key.toString();
				String namedOutput = "";
				
				if ("www.guazi.com".equals(domain) || "www.renrenche.com".equals(domain)) {
					namedOutput = "ershouche";
				} else if ("www.kaola.com".equals(domain) || "www.fengqu.com".equals(domain)) {
					namedOutput = "haigou";
				} else if ("gz.jz.jiehun.com.cn".equals(domain) || "www.homekoo.com".equals(domain)
						|| "www.aijaw.com".equals(domain) || "www.yigaojiaju.com".equals(domain)
						|| "www.niernuojiaju.com".equals(domain) || "www.ebery.com.cn".equals(domain)
						|| "www.artist88.com".equals(domain) || "www.easily-china.com".equals(domain)
						|| "www.wy100.com".equals(domain) || "www.suofeiya.com.cn".equals(domain)) {
					namedOutput = "haigou";
				} else if ("eastmoney.com".equals(domain) || "pay.gw.com.cn".equals(domain) 
						|| "i.10jqka.com.cn".equals(domain) || "pass.10jqka.com.cn".equals(domain)) {
					namedOutput = "licai";
				} else if ("search.xcar.com.cn".equals(domain) || "newcar.xcar.com.cn".equals(domain)) {
					namedOutput = "carinfo";
				} else if ("www.rong360.com".equals(domain) || "www.huirendai.com".equals(domain)
						|| "www.yonglibao.com".equals(domain) || "www.jiuxindai.com".equals(domain)) {
					namedOutput = "wangdai";
				} else if ("www.weimob.com".equals(domain)) {
					namedOutput = "weixin";
				} else {
					namedOutput = "other";
				}
				output.write(namedOutput,value,new Text(""));
//				context.write(value, new Text(""));
				
			}
		}
		
		@Override
	    protected void setup(Context context
	    ) throws IOException, InterruptedException {
	        output = new MultipleOutputs<Text, Text>(context);
	    }
		
		@Override
		protected void cleanup(Context context) throws IOException,
		InterruptedException {
			output.close();
		}
	}

	
	/** 初始化JOB
	 * @param
	 */
	public Job UserMDFJob(final Configuration conf,final String[] args) throws IOException {		
		
		String input = args[1];
		System.out.println("input:" + input);
		System.out.println("output:" + args[0]);
		Job job = Job.getInstance(conf);
		job.setJobName("Ecommerce");
		job.setJarByClass(PostBaseDriver.class);
		for (String pt : input.split(",")) {
			FileInputFormat.addInputPath(job,new Path(pt));
		}
		
		job.setMapperClass(UserMDFMapper.class);
		Path outputpath = new Path(args[0]);
		outputpath.getFileSystem(conf).delete(outputpath, true);
		FileOutputFormat.setOutputPath(job, outputpath);
		job.setInputFormatClass(TextInputFormat.class);
		//job.setInputFormatClass(OrcNewInputFormat.class);		
		//FileOutputFormat.setCompressOutput(job, true);
		       
		MultipleOutputs.addNamedOutput(job,"ershouche",TextOutputFormat.class,Text.class,Text.class);  
		MultipleOutputs.addNamedOutput(job,"haigou",TextOutputFormat.class,Text.class,Text.class);
		MultipleOutputs.addNamedOutput(job,"jiaju",TextOutputFormat.class,Text.class,Text.class);
		MultipleOutputs.addNamedOutput(job,"licai",TextOutputFormat.class,Text.class,Text.class);
		MultipleOutputs.addNamedOutput(job,"carinfo",TextOutputFormat.class,Text.class,Text.class);
		MultipleOutputs.addNamedOutput(job,"wangdai",TextOutputFormat.class,Text.class,Text.class);
		MultipleOutputs.addNamedOutput(job,"weixin",TextOutputFormat.class,Text.class,Text.class);
		MultipleOutputs.addNamedOutput(job,"other",TextOutputFormat.class,Text.class,Text.class);
		
		job.setReducerClass(UserMDFReducer.class);
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
		log.info("Usage: hadoop jar ecommerce.jar com.aotain.project.ecommerce.PostBaseDriver  <out> <in> <post_rule.xml> <post_person.xml> <citycode> <partdate>");

	}
	
	public static void main(final String[] args) throws Exception {
		log.info("-------UserMDF main start----");
		
		int exitCode = ToolRunner.run(new PostBaseDriver(), args);
		
		System.exit(exitCode);
//		new UserMDFMapper().loadConfRule() ;
	}
	
	/** 
	 * @param
	 *  args[0]=outputpath args[1]=inputpath args[2]=post_rule.xml args[3]=post_person.xml args[4]=citycode args[5]=partdate
	 */
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();	
		   conf.set("mapreduce.input.fileinputformat.input.dir.recursive","true");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		for(int i=0;i<otherArgs.length;i++){
			log.info(otherArgs[i]);
		}
		if (otherArgs.length != 6) {
			usage("Wrong number of arguments: " + otherArgs.length);
			System.exit(-1);
		}
		conf.set("post_rule_conf", otherArgs[2]);
		conf.set("post_rule_person", otherArgs[3]);
		conf.set("citycode", otherArgs[4]);
		conf.set("partdate", otherArgs[5]);
		
		Job statics = UserMDFJob(conf,otherArgs);
		int ret = statics.waitForCompletion(true) ? 0 : 1;		
		return ret;
		
	}
}





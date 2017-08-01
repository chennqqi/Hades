package com.aotain.project.ecommerce;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.aotain.common.HFileConfigMgr;
import com.aotain.common.HFileConfigMgr.FieldItem;
import com.aotain.project.ecommerce.pojo.Pair;
import com.aotain.project.ecommerce.rule.Rule;
import com.aotain.project.ecommerce.rule.RuleManagerInfo;

/**
 * 数据提取：根据规则，输出postcont字段里指定的属性及属性值
 * 数据源：to_opr_postorig表
 * 如postcont字段值 mobile=13421812993&user_name=ace，根据规则可能输出 Phone=User13421812993,Name=ace
 * 不包含规则的数据过滤
 * @author Ace
 *
 */
public class PostFormatInfo  extends Configured implements Tool {

	private static final Log log = LogFactory.getLog(PostFormatInfo.class);
	private static Configuration conf = null;
	
	public static class UserMDFMapper extends Mapper<LongWritable, Text, Text, Text> {
		private static  RuleManagerInfo ruleManager = new RuleManagerInfo();


		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] items = value.toString().split("\\|");
	
			if(items.length < 14) {  //测试环境16个长度  生产环境14个长度
				return;
			}
			String BroadbandAccount = items[0];  // 输出
			
			String domain = items[6];
			String postcon = items[13];

			
			String replacedCol = findAndReplaceColumn(postcon, domain);
			if (replacedCol.equals("")) {
				return;
			}
			
			StringBuffer outkey = new StringBuffer();
			outkey.append(BroadbandAccount).append("|");
			
			outkey.append(replacedCol);
			
			String keys=outkey.toString();
			 String [] info=keys.split("\\|");
			
			 if(info.length!=1)
			 context.write(new Text(keys), new Text(""));
			 
			/*String keys  = outkey.toString();
			System.out.println(keys);
			context.write(new Text(keys), new Text(""));*/

		}

		private String findAndReplaceColumn(String postcon, String host) {
			String[] columns = {"Phone","Mail","QQAccount"};
			String result = "";
			Rule rule = ruleManager.getRule(host);
			String needValue="";
			String column_key="";
			if(null != rule) {                
				for(String column:columns){
					column_key=rule.getPairKFromPairV(column);
					String needRule = "(&|^)" +column_key + "=(.*?)(&|$)";// 需要查找的正则表达式 
					
					try {
						needValue = findMathc(postcon, needRule,column);
						needValue=check(column, needValue);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					result=result+needValue+"|";
				}
				
				
			}
		
			return result;
		}
		

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			 // String sColumns = context.getConfiguration().get("app.columns");
			 //columns = sColumns.split(",");
			conf = context.getConfiguration();
			String[] columns = {"Phone","Mail","QQAccount"};
			loadConfRuleNew(null, columns);
			
		}

	 /**
	  * 按规则查找
	  * @param url  如mobile=13421812993&id_md5=359a3d10248653da268661947a7cece
	  * @param regex 如 mobile=(.*)&
	  * @return  如 13421812993
	 * @throws IOException 
	  */
		private String findMathc(String url,String regex,String columns) throws IOException{
			 /*Pattern p = Pattern.compile(regex,Pattern.CASE_INSENSITIVE);
		        Matcher m = p.matcher(url); 
		        String match="";
		      
		        if (m.find()) {
		        	match=m.group(2);
		        } 
		        return match;*/
		        Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
				Matcher m = p.matcher(url);
				String match = "";
				String realvalue="";
				int i=0;
				while (m.find()) {
					if(i>=1) {
						match +="#";
					}
					i++;
					realvalue=check(columns, m.group(2));
					match = match + realvalue;
				}
				return match;
		}
		
		/**
		 * 装载配置到内存
		 * post_rule.xml 每个domain对应一个urlid
		 * post_person.xml urlid对应POST_FIELD(post表字段名)和COMM_FIELD(输出字段名)
		 * @throws IOException
		 */
		private static void loadConfRuleNew(String[] domains, String[] columns) throws IOException {
			String post_rule = conf.get("post_rule_conf");
			String post_person = conf.get("post_rule_person");
			FileSystem post_rule_fs = FileSystem.get(URI.create(post_rule), conf);
			FileSystem post_person_fs = FileSystem.get(URI.create(post_person), conf);
			SAXReader saxReader = new SAXReader();
			List<Pair> rules = new ArrayList<Pair>();
			try {
				Document document = saxReader.read(post_rule_fs.open(new Path(post_rule)));
				Element rootElement = document.getRootElement();
				List rows = rootElement.elements();
				for (int i = 0, l = rows.size(); i < l; i++) {
					Element row = (Element) rows.get(i);
					String urlId = row.elementText("URL_ID");
					String domain = row.elementText("DOMAIN");
					// 如果限制了domain，只装载domain相关的rule,否则全量装载
					if(null == domains || domains.length == 0) {
						rules.add(new Pair(urlId.trim(), domain.trim()));
					} else {
						for(String domainstr : domains) {
							if(domain.contains(domainstr)) {
								rules.add(new Pair(urlId.trim(), domain.trim()));
								break;
							}
						}
					} 
				}
				document = saxReader.read(post_person_fs.open(new Path(post_person)));
				rootElement = document.getRootElement();
				rows = rootElement.elements();
				
				for (int i = 0, l = rows.size(); i < l; i++) {
					Element row = (Element) rows.get(i);
					String urlId = row.elementText("URL_ID");
					String postFileId = row.elementText("POST_FIELD");
					String commFileId = row.elementText("COMM_FIELD");
					
					// 如果限制了列，只装载有对应列的rule,否则全量装载
					boolean add = false;
					if(null == columns || columns.length == 0) {
						add = true;
					}else {
						for(String col : columns) {
							if(commFileId.equalsIgnoreCase(col)) {
								add = true;
							}
						}
					}
					
					if(add) {
						for (Pair p : rules) {
							if (p.getKey().equals(urlId)) {
								ruleManager.addRule(p.getValue(), postFileId, commFileId);
							}
						}
					}
				}
			} catch (DocumentException e) {
				System.out.println("解析文件异常" + e.toString());
				throw new IOException(e);
			}
			ruleManager.printRule();
		}
		
		
	


		
		
	}
	
	
	public static class UserMDFReducer extends Reducer<Text, Text, Text, Text> {	
		
		
		protected void reduce(Text key,  Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

			context.write(key, new Text(""));
			
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
		job.setJobName("Post Info");
		job.setJarByClass(PostFormatInfo.class);
		for (String pt : input.split(",")) {
			FileInputFormat.addInputPath(job,new Path(pt));
		}
		
		job.setMapperClass(UserMDFMapper.class);
		Path outputpath = new Path(args[0]);
		outputpath.getFileSystem(conf).delete(outputpath, true);
		FileOutputFormat.setOutputPath(job, outputpath);
		job.setInputFormatClass(TextInputFormat.class);
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
		log.info("Usage: hadoop jar ecommerce.jar com.aotain.project.ecommerce.PraseProvince  <out> <in> <post_rule.xml> <post_person.xml> <citycode> <partdate>");

	}
	
	public static void main(final String[] args) throws Exception {
		log.info("-------UserMDF main start----");
		
		int exitCode = ToolRunner.run(new PostFormatInfo(), args);
		
		System.exit(exitCode);
//		new UserMDFMapper().loadConfRule() ;
	}
	
	/** 
	 * @param
	 *  args[0]=outputpath args[1]=inputpath args[2]=post_rule.xml args[3]=post_person.xml 
	 */
	public int run(String[] args) throws Exception {
		conf = new Configuration();	
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
		
			
		Job statics = UserMDFJob(conf,otherArgs);
		int ret = statics.waitForCompletion(true) ? 0 : 1;		
		return ret;
		
	}
	private static String check( String columns,String value) throws IOException {//{"Phone","Mail","QQAccount"};
		value=value.split(";")[0];
		value=value.trim().toLowerCase();
		//value=transcoding(value,"utf-8");//转义解码
		if(columns.equals("Phone")){
			value=findByRegex(value, "^((13[0-9])|(15[^4,\\D])|(18[0,5-9]))\\d{8}$", 0);
		}
		else if(columns.equals("Mail")){
			value=findByRegex(value, "^([a-zA-Z0-9_\\-\\.]{1,50})@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.)|(([a-zA-Z0-9\\-]+\\.)+))(com|cn|net|com.cn|org|gd)(\\]?)$", 0);
		}
		
		if(value==null){
			value="";
		}
		return value;
	}
	 public static String findByRegex(String str, String regEx, int group)
	 	{
	 		String resultValue = null;
	 		if ((str == null) || (regEx == null) || ((regEx != null) && ("".equals(regEx.trim())))) 
	 			return resultValue;
	 		
	 		
	 		Pattern p = Pattern.compile(regEx);
	 		Matcher m = p.matcher(str);
	 		

	 		boolean result = m.find();
	 		System.out.println("result "+result);
	 		if (result)
	 		{
	 			resultValue = m.group(group);
	 			System.out.println(resultValue);
	 		}
	 		return resultValue;
	 	}
	 public static String transcoding(String value,String code)
	 	{
			if (value.contains("%")){  //解码
				value = value.replaceAll("%(?![0-9a-fA-F]{2})", "%25");  
				try {
					value = java.net.URLDecoder.decode(value, code);
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
	 		return value;
	 	}
}





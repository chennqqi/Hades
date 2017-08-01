package com.aotain.project.dmp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
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

public class OtherHouseData extends Configured implements Tool {

	private final static String cut = "|";
	private static final Log log = LogFactory.getLog(OtherHouseData.class);
	private static Configuration conf = null;

	public static class UserMDFMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		public Map<String, String> domainMap = new HashMap<String, String>();

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			domainMap
					.put("58.com",
							"二手房,(http://sz\\.58\\.com/ershoufang/[0-9]+x\\.shtml)\\??|(http://sz\\.58\\.com/[a-z]*/?ershoufang/?[0-9a-z]*)\\??,1");
			domainMap.put("ifeng.com", "新房,1,2");
			domainMap
					.put("ifeng.com/sale",
							"新房,(http://sz\\.house\\.ifeng\\.com/sale/search/36688/_/_/[0-9_]+\\.shtml)\\??,1");
			domainMap
					.put("ifeng.com/homedetail",
							"新房,(http://sz\\.house\\.ifeng\\.com/homedetail/[0-9]+)\\??,1");
			domainMap
					.put("ifeng.com/world",
							"海外房产,(http://world\\.house\\.ifeng\\.com/world/[A-Z]+/_/_/[0-9_]+\\.shtml)\\??|(http://world\\.house\\.ifeng\\.com/world/homedetail/[0-9]+)\\??,1");
			domainMap
					.put("ifeng.com/detail",
							"海外房产,(http://world\\.house\\.ifeng\\.com/detail/[0-9_]+/[0-9_]+\\.shtml)\\??|(http://sz\\.house\\.ifeng\\.com/detail/[0-9_]+/[0-9_]+\\.shtml)\\??,1");

			domainMap.put("centanet.com", "新房,1,2");
			domainMap
					.put("centanet.com/xinfang",
							"新房,(http://sz\\.centanet\\.com/xinfang/lp-[0-9A-Z]+/?)\\??|(http://sz\\.centanet\\.com/xinfang/[a-z]*/?[0-9a-z]*/?)\\??,1");
			domainMap
					.put("centanet.com/ershoufang",
							"二手房,(http://sz\\.centanet\\.com/ershoufang/[-0-9a-z]+\\.html)\\??|(http://sz\\.centanet\\.com/ershoufang/[a-z]*/?[0-9a-z]*/?)\\??,1");
			domainMap
					.put("centanet.com/xzl",
							"新房,(http://sz\\.centanet\\.com/xzl/BusinessShop/Search/[-_%0-9a-zA-Z]+)\\??|(http://sz\\.centanet\\.com/xzl/BusinessShop/[dD]+etails/[-0-9a-z]+)\\??,1");
			domainMap
					.put("centanet.com/xiaoqu",
							"二手房,(http://sz\\.centanet\\.com/xiaoqu/[a-z]*/?[0-9a-z]*/?)\\??|(http://sz\\.centanet\\.com/xiaoqu/[a-z]+-[a-z]+/?)\\??,1");
			domainMap.put("qq.com", "新房,1,3");
			domainMap.put("db.house.qq.com",
					"新房,(http://db\\.house\\.qq\\.com/sz_[0-9]+/?)\\??,1");
			domainMap
					.put("esf.db.house.qq.com",
							"二手房,(http://esf\\.db\\.house\\.qq\\.com/sz/sale/[0-9]+_[0-9]+/?)\\??,1");
			domainMap.put("ganji.com", "新房,1,2");
			domainMap
					.put("ganji.com/fang5",
							"新房,(http://sz\\.ganji\\.com/fang5/[0-9]+x\\.htm)\\??|(http://sz\\.ganji\\.com/fang5/[a-z]*/?[a-z0-9]*/?)\\??,1");
			domainMap.put("163.com", "新房,1,3");
			domainMap
					.put("xf.house.163.com",
							"新房,(http://xf\\.house\\.163.com/sz/search/[0-9-]+\\.html)\\??|(http://xf\\.house\\.163.com/sz/[A-Z0-9]+\\.html)#?,1");
			domainMap
					.put("vanke.com",
							"新房,(http://life\\.vanke\\.com/web/goodsDetail\\?GoodsID=[0-9]+&GoodsType=[0-9]+)&?,1");
			domainMap.put("jjshome.com", "新房,1,2");
			domainMap
					.put("jjshome.com/ysl",
							"新房,(http://shenzhen\\.jjshome\\.com/ysl/[0-9]+)\\??|(http://shenzhen\\.jjshome\\.com/ysl/index/[0-9a-z]+)\\??,1");
			domainMap
					.put("jjshome.com/esf",
							"二手房,(http://shenzhen\\.jjshome\\.com/esf/[0-9]+)\\??|(http://shenzhen\\.jjshome\\.com/esf/index/[0-9a-z]+)\\??,1");
			domainMap.put("fangdd.com", "新房,1,3");

			domainMap.put("m.fangdd.com", "新房,(http://m.fangdd.com/nh),1");
			domainMap.put("hd.xf.fangdd.com", "新房,(http://hd.xf.fangdd.com),1");
			domainMap.put("fs.esf.fangdd.com",
					"二手房,(http://fs.esf.fangdd.com),1");
			domainMap
					.put("xf.fangdd.com",
							"新房,(http://xf\\.fangdd\\.com/[a-z]+/[0-9]+\\.html)\\??|(http://xf\\.fangdd\\.com/shenzhen/loupan/[-0-9a-z]+)\\??,1");
			domainMap
					.put("esf.fangdd.com",
							"二手房,(http://esf\\.fangdd\\.com/[a-z]+/[0-9]+\\.html)\\??|(http://esf\\.fangdd\\.com/shenzhen/list/[-_%A-Z0-9a-z]+)\\??,1");
			domainMap.put("baidu.com", "新房,1,3");
			domainMap
					.put("fang.baidu.com",
							"新房,(http://fang\\.baidu\\.com/secondhouse\\?uid=[0-9]+&uid_type=house&estate_id=[0-9]+&city_id=340)\\??,1");
			domainMap.put("szhome.com", "新房,1,2");
			domainMap.put("szhome.com/house",
					"新房,(http://m\\.leju\\.com/touch/house/sz/[0-9]+/?)\\??,1");
			domainMap.put("szhome.com/sell",
					"二手房,(http://esf\\.szhome\\.com/sell/[0-9]+\\.html)\\??,1");
			domainMap.put("leju.com", "新房,1,4");
			domainMap.put("leju.com/touch/house",
					"新房,(http://m\\.leju\\.com/touch/house/sz/[0-9]+/?)\\??,1");
			domainMap
					.put("leju.com/touch/esf",
							"二手房,(http://m\\.leju\\.com/touch/esf/sz/detail/[0-9]+)\\??|(http://m\\.leju\\.com/touch/esf/sz/detail/[-0-9a-z]+/?)\\??,1");
			domainMap.put("anjuke.com", "新房,1,5");
			domainMap.put("api.anjuke.com",
					"新房,(http://api.anjuke.com/mobile/v5/recommend/sale),1");
			domainMap
					.put("m.anjuke.com", "海外房产,(http://m.anjuke.com/haiwai),1");
			domainMap
					.put("s.anjuke.com",
							"海外房产,(http://s.anjuke.com/st?__site=m_anjuke&p=Haiwai)|(http://s.anjuke.com/st?__site=m_anjuke-npv&p=Haiwai),1");
			domainMap
					.put("api.fang.anjuke.com",
							"新房,(http://api.fang.anjuke.com/m/android/1.3/loupan/newlistv2)|(http://api.fang.anjuke.com/m/android/1.3/loupan/Newimages)|(http://api.fang.anjuke.com/m/android/1.3/loupan/lessGussLike),5");
			domainMap
					.put("anjuke.com/haiwai",
							"海外房产,(http://[a-z]+\\.haiwai\\.anjuke\\.com/detail/building-[0-9]+\\.html)\\??,1");
			domainMap.put("fang.com", "新房,1,5");
			domainMap
					.put("fang.com/house",
							"新房,(http://[0-9a-z]+\\.fang\\.com/house/[0-9_]+\\.htm)\\??|(http://[a-z]+\\.fang\\.com/house/[0-9]+\\.htm)\\??,1");
			domainMap
					.put("shop.fang.com",
							"商业地产,(http://shop\\.fang\\.com/shou/[0-9_]+\\.html)\\??,1");
			domainMap
					.put("office.fang.com",
							"商业地产,(http://office\\.fang\\.com/zu/[0-9_]+\\.html)\\??,1");
			domainMap
					.put("m.fang.com",
							"新房,(http://m.fang.com/world)|(http://m.fang.com/fangjia/?c=pinggu&a=worldFangjia&src=client]),1");
			domainMap
					.put("soufunapp.3g.fang.com",
							"新房,(http://soufunapp.3g.fang.com/http/sf2014.jsp?city=%E6%B7%B1%E5%9C%B3&location=newhouse)|(http://soufunapp.3g.fang.com/http/sfservice.jsp?channel=houselist&city=%E6%B7%B1%E5%9C%B3&housetype=xf)|(http://soufunapp.3g.fang.com/http/sfservice.jsp?city=%E6%B7%B1%E5%9C%B3&fromType=1&messagename=getNewHousePriceData),1");
			domainMap.put("soufunappxf.3g.fang.com",
					"新房,(http://soufunappxf.3g.fang.com),1");
			domainMap.put("lianjia.com", "新房,1,5");
			domainMap
					.put("lianjia.com/house",
							"海外房产,(http://[a-z]+\\.lianjia\\.com/house/[A-Z0-9]+\\.html)\\??,1");
			domainMap
					.put("image2.lianjia.com",
							"新房,(http://image2.lianjia.com/newhcms|http://image2.lianjia.com/x-xf),1");
			domainMap.put("image3.lianjia.com",
					"新房,(http://image3.lianjia.com/xf-resblock),1");
			domainMap.put("qfang.com", "新房,1,6");
			domainMap.put("qfang.com/appapi/v4_2/newHouse",
					"新房,(http://shenzhen.qfang.com/appapi/v4_2/newHouse),1");
			domainMap.put("qfang.com/appapi/v4_2/room",
					"二手房,(http://shenzhen.qfang.com/appapi/v4_2/room),1");
			domainMap.put("qfang.com/appapi/v4_2/office",
					"商业地产,(http://shenzhen.qfang.com/appapi/v4_2/office),1");
			domainMap.put("qfang.com/appapi/v4_2/enums",
					"商业地产,(http://shenzhen.qfang.com/appapi/v4_2/enums),1");
			domainMap
					.put("qfang.com/office/rent",
							"商业地产,(http://shenzhen\\.qfang\\.com/office/rent/[0-9]+)\\??,1");
			domainMap
					.put("qfang.com/office/sale",
							"商业地产,(http://shenzhen\\.qfang\\.com/office/sale/[0-9]+)\\??,1");

		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			try {

				String line = value.toString();

				String items[] = line.split("\\|", -1);
				if (items.length == 20) {
					StringBuilder sb = new StringBuilder();
					String url = items[4];
					String domain = items[3];
					String accesstime = items[11];
					String username = items[1];
					String sys = items[6].toLowerCase().trim();
					int flag = 2;
					if (sys.equals("windows") || sys.equals("macintosh")) {
						flag = 1;
					}

					String domains[] = domain.split("\\.");
					String rootDomain = domains[domains.length - 2] + "."
							+ domains[domains.length - 1];
					String rules = domainMap.get(rootDomain);
					String referDomain = items[15];
					if (referDomain.contains(rootDomain)
							|| " ".equals(referDomain)) {
						referDomain = "1";
					}
					boolean dflag = !domain.equals(rootDomain);
					if (rules != null) {
						String[] result = paraseRules(rules, url);
						String match = "";
						int jflag = Integer.parseInt(result[2]);
						if (jflag == 1) {
							match = result[3];
						} else if (jflag == 2 && dflag) {
							try {
								String paths[] = new URL(url).getPath().split(
										"/");
								if (paths.length > 1) {
									rootDomain = rootDomain + "/" + paths[1];
									rules = domainMap.get(rootDomain);
									if (rules != null) {
										String result2[] = paraseRules(rules,
												url);
										match = result2[3];
										if ("".equals(match)) {
											match = "其他";
										}
									}
								}
							} catch (MalformedURLException e) {
								// TODO Auto-generated catch block
								// e.printStackTrace();
							}
						} else if (jflag == 3 && dflag) {
							rootDomain = domain;
							rules = domainMap.get(rootDomain);
							if (rules != null) {
								String result2[] = paraseRules(rules, url);
								match = result2[3];
								if ("".equals(match)) {
									match = "其他";
								}
							}

						} else if (jflag == 4 && dflag) {
							try {
								String paths[] = new URL(url).getPath().split(
										"/");
								if (paths.length > 2) {
									rootDomain = rootDomain + "/" + paths[1]
											+ "/" + paths[2];
									;
									rules = domainMap.get(rootDomain);
									if (rules != null) {
										String result2[] = paraseRules(rules,
												url);
										match = result2[3];
										if ("".equals(match)) {
											match = "其他";
										}
									}
								}
							} catch (MalformedURLException e) {
								// TODO Auto-generated catch block
								// e.printStackTrace();
							}
						} else if (jflag == 5 && dflag) {
							rootDomain = domain;
							rules = domainMap.get(rootDomain);
							if (rules != null) {
								String result2[] = paraseRules(rules, url);
								match = result2[3];

								if ("".equals(match)) {
									match = "其他";
								}
							} else {

								String paths[] = new URL(url).getPath().split(
										"/");
								if (paths.length > 1) {
									rootDomain = rootDomain + "/" + paths[1];
									;
									rules = domainMap.get(rootDomain);
									if (rules != null) {
										String result2[] = paraseRules(rules,
												url);
										match = result2[3];
										if ("".equals(match)) {
											match = "其他";
										}
									}
								}
							}
						} else if (jflag == 6 && dflag) {
							try {
								String paths[] = new URL(url).getPath().split(
										"/");
								if (paths.length > 2) {
									if (paths.length > 3) {
										rootDomain = rootDomain + "/"
												+ paths[1] + "/" + paths[2]
												+ "/" + paths[3];
										rules = domainMap.get(rootDomain);
										if (rules != null) {
											String result2[] = paraseRules(
													rules, url);
											match = result2[3];
											if ("".equals(match)) {
												match = "其他";
											}
										} else {
											rootDomain = rootDomain + "/"
													+ paths[1] + "/" + paths[2];
											;
											rules = domainMap.get(rootDomain);
											if (rules != null) {
												String result2[] = paraseRules(
														rules, url);
												match = result2[3];
												if ("".equals(match)) {
													match = "其他";
												}
											}
										}
									}
								}
							} catch (MalformedURLException e) {
								// TODO Auto-generated catch block
								// e.printStackTrace();
							}
						}

						String outkey = username + "|" + url + "|" + match
								+ "|房产|" + domain + "|" + accesstime + "|"
								+ flag + "|" + referDomain + "|";
						if (!"".equals(match) && username.contains("@"))
							context.write(new Text(username), new Text(outkey));
					}

				}
			} catch (Exception e) {

			}

		}
	}

	private static String[] paraseRules(String rules, String url) {
		String[] result = new String[4];
		String source = rules.split(",")[0];
		String regex = rules.split(",")[1];
		String jflag = rules.split(",")[2];
		result[0] = source;
		result[1] = regex;
		result[2] = jflag + "";

		String match = "";
		if ("1".equals(jflag)) {
			if (regex.contains("|")) {
				String regexs[] = regex.split("\\|");
				for (int i = 0; i < regexs.length; i++) {
					match = findMathc(url, regexs[i]);
					if (!"".equals(match)) {
						match = result[0];
						break;
					}
				}
			} else {
				match = findMathc(url, regex);
				if (!"".equals(match)) {
					match = result[0];

				}
			}

		}

		result[3] = match;
		return result;
	}

	private static boolean isMatch(String url, String regex) {
		Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(url);
		return m.find();
	}

	private static String findMathc(String url, String regex) {
		Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(url);
		String match = "";

		while (m.find()) {
			match = m.group(1);
		}
		return match;
	}

	public static class UserMDFReducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text value : values) {
				context.write(value, new Text(""));
			}
			// context.write(key,new Text(""));

		}
	}

	/**
	 * 初始化JOB
	 * 
	 * @param args[0]=domain args[1]=outputpath args[2]=inputpath jarname
	 *        usermd5 /user/data/usermdf/out/20151201
	 *        /user/hive/warehouse/broadband.db/to_opr_http/shenzhen/20151201
	 */
	public Job UserMDFJob(final Configuration conf, final String[] args)
			throws IOException {

		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
		String[] statstamp = args[2].split("/");
		System.out.println();
		String jobname = ">>>House_data>>> " + args[0] + ">>>"
				+ statstamp[statstamp.length - 1];
		String input = args[2];
		Job job = Job.getInstance(conf);
		job.setJobName(jobname);
		job.setJarByClass(OtherHouseData.class);
		for (String pt : input.split(",")) {
			FileInputFormat.addInputPath(job, new Path(pt));
		}

		job.setMapperClass(UserMDFMapper.class);
		Path outputpath = new Path(args[1]);
		outputpath.getFileSystem(conf).delete(outputpath, true);
		FileOutputFormat.setOutputPath(job, outputpath);
		// job.setInputFormatClass(TextInputFormat.class);
		job.setInputFormatClass(LzoTextInputFormat.class);
		// FileOutputFormat.setCompressOutput(job, true);
		// FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);

		job.setReducerClass(UserMDFReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job;
	}

	/**
	 * @param errorMsg
	 *            Error message. Can be null.
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
		int exitCode = ToolRunner.run(new OtherHouseData(), args);

		System.exit(exitCode);
	}

	/**
	 * @param args[0]=jobname args[1]=outputpath args[2]=inputpath
	 *        args[3]=impalanode args[4]=province
	 */
	public int run(String[] args) throws Exception {
		conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		for (int i = 0; i < otherArgs.length; i++) {
			log.info(otherArgs[i]);
		}
		if (otherArgs.length != 5) {
			usage("Wrong number of arguments: " + otherArgs.length);
			System.exit(-1);
		}
		String city = otherArgs[3];
		String partdate = otherArgs[4];
		conf.set("city", city);
		conf.set("partdate", partdate);
		Job statics = UserMDFJob(conf, otherArgs);
		int ret = statics.waitForCompletion(true) ? 0 : 1;
		return ret;

	}
}

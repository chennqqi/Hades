package dmpcommon.test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import dmpcommon.KVConfig;
import dmpcommon.KVReducer;
import dmpcommon.KVTextMapper;
public class TestMR {
	Configuration conf = new Configuration();
	MapDriver<LongWritable,Text,Text,Text> mapDriver;
	ReduceDriver<Text,Text,Text,Text> reduceDriver;

	@Before
	public void setUp() throws Exception {
		KVTextMapper mapper = new KVTextMapper();
		mapDriver = MapDriver.newMapDriver(mapper);
		Configuration conf = mapDriver.getConfiguration();
		KVConfig kvConf = new KVConfig("conf/to_opr_http.cfg","conf/kv.csv","conf/ua_device.conf","conf/devicecheck_�ֹ�","",conf);
		kvConf.initConfig();
		
		/*KVReducer reducer = new KVReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		conf = reduceDriver.getConfiguration();
		conf.set("date", "20170322");*/
	}
	
	@Test
	public void testMapper() throws IOException {
		mapDriver.addInput(new LongWritable(), new Text("61.173.95.33	83af1ea104879c453bdc2f7f9651ce9f1db8cc62	1490820229975	http://is.snssdk.com/promotion/app/lt/?iid=9011307234&device_id=34980837857&ac=wifi&mac=6C:25:B9:B8:9C:CA&channel=huawei&aid=13&app_name=news_article&version_code=607&version_name=6.0.7&device_platform=android&ssmix=a&device_type=HUAWEI+TAG-AL00&device_brand=HUAWEI&language=zh&os_api=22&os_version=5.1&uuid=862772039741781&openudid=1cf51aab9dafa244&manifest_version_code=607&resolution=720*1184&dpi=320&update_version_code=6075&_rticket=1490820214128	NoDef	b2todHRwLzMuNC4x	180.97.168.168	X2JhPUJBMC4yLTIwMTcwMjEwLTUxZTMyLVI0TlJ3UkZPNllVbFlPTG5OTUNwOyBfZ2E9R0ExLjIuMjI2MzIzMTExLjE0ODUzNDczOTA7IGFsZXJ0X2NvdmVyYWdlPTg0OyBxaFszNjBdPTE7IGluc3RhbGxfaWQ9OTAxMTMwNzIzNDsgdHRyZXE9MSRjNThhNzNjZmZlMDI2ZGY5YmRmNTcyMWNmZTdhNTczOGNmYjk2OTA4	33724"));
		mapDriver.addInput(new LongWritable(), new Text("61.185.255.147	none	1490818132072	http://irs01.com/hvt?_ua=UA-iqiyi-140002&type=end&v_id=330590800&_t=i&_z=m&idfa=QxbWIVSMpybZt2LCJLBS9KpTbnQOmrwyEEhwv69Y8K7k4rZiLPs5knZx43hDxQFv8pX8hkcZvjT%252Bwi0Oj%252F5GKA%253D%253D&openudid=ZstcUwo3FyKpixcsjGFFq2pHF2QzLpXkoFjjq8%252FMz5XSFDYy2s90pVKf9gN0JfjzUUkXveN1qWYBDdeRlqOSJA%253D%253D&device_name=iPhone9,1&sr=750*1334&spend=82084&len=0&progress=0&upid=396a0bcef305aff84d3d34bfbca37ba1&encrypt=1&os=1	NoDef	UUlZSVZpZGVvLzYuMi4wIChpT1M7Y29tLnBwcy50ZXN0O2lPUzEwLjIuMTtpUGhvbmU5LDEpIENvcmVqYXI=	180.169.19.132	X2l3dF9pZD1oQktwdEFqX3VsaXhtZWlsUjFYSkdBQQ==	5714"));
//		mapDriver.addInput(new LongWritable(), new Text("440300|075508102118@163.gd|121.15.1.1|www.olosee.com|http://ping.pinyin.sogou.com/app_name=shop_iphonedasdada/pingback.gif?h=5B7C9463CAC60460CC242CDA89489A03&mac=6C:25:B9:B8:9C:CA&v=8.0.0.8439&r=5282__5.1.0&id=Y10000001&passport=&type=predownload&ppversion=3.1.0.1972|http://| | | | | |20170228150057|IPLOC=CN4403; YYID=5B7C9463CAC60460CC242CDA89489A03; IMEVER=8.0.0.8439| |0| |0|1P_SapphireWallpapers/1.4 (iPhone; iPhone OS 8.1.2; zh-Hans; iPhone7,iPhone7 p2|106.120.151.146|b0a|50|shenzhen|20161023"));
//		mapDriver.addInput(new LongWritable(), new Text("440300|075508102118@163.gd|121.15.1.1|www.olosee.com|http://ping.pinyin.sogou.com/app_name=shop_iphonedasdada/pingback.gif?h=5B7C9463CAC60460CC242CDA89489A03&mac=6C:25:B9:B8:9C:CA&v=8.0.0.8439&r=5282__5.1.0&id=Y10000001&passport=&type=predownload&ppversion=3.1.0.1972|http://| | | | | |20170228003057|IPLOC=CN4403; YYID=5B7C9463CAC60460CC242CDA89489A03; IMEVER=8.0.0.8439| |0| |0|1P_SapphireWallpapers/1.4 (iPhone; iPhone OS 8.1.2; zh-Hans; iPhone7,iPhone7 p2|106.120.151.146|b0a|50|shenzhen|20161023"));
//		mapDriver.addInput(new LongWritable(), new Text("440300|075508102118@163.gd|121.15.1.1|www.olosee.com|http://ping.pinyin.sogou.com/app_name=shop_iphonedasdada/pingback.gif?h=5B7C9463CAC60460CC242CDA89489A03&mac=6C:25:B9:B8:9C:CA&v=8.0.0.8439&r=5282__5.1.0&id=Y10000001&passport=&type=predownload&ppversion=3.1.0.1972|http://| | | | | |20170228003057|IPLOC=CN4403; YYID=5B7C9463CAC60460CC242CDA89489A03; IMEVER=8.0.0.8439| |0| |0|1P_SapphireWallpapers/1.4 (iPhone; iPhone OS 8.1.2; zh-Hans; iPhone7,iPhone7 p2|106.120.151.146|b0a|50|shenzhen|20161023"));
//		mapDriver.withInput(new LongWritable(), new Text("440300|075508102118@163.gd|113.90.233.254|msg.71.am|http://msg.71.am/core?t=5&a=7&ra=1&va=0&pf=2&p=22&p1=222&p2=3000&sdktp=1&c1=2&r=620614500&aid=620614500&u=869918021574267&pu=&v=8%2E1&krv=3%2E8%2E2&dt=&hu=-1&rn=1488286444&islocal=0&as=cbfd3b8126b189561f200c53f52ec4b3&ve=847fe0e92253127af89704103820c0b2&pe=&vfrm=&chl=&hcd|http://|android|3.8.2| | | |20170228205404| | |0| |0|QYPlayer/Android/3.8.2/nt_1|106.38.219.49|2c72|50|shenzhen|20170301"));
		mapDriver.withOutput(new Text("Phone=13421812993,UserName=ace"), new Text(""));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() throws IOException{
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("2|www.olosee.com|21_01"));
		values.add(new Text("1|www.olosee.com|8_50"));
		values.add(new Text("1|www.olosee.com|15_00"));
		values.add(new Text("2|www.olosee.com|0_30"));
		reduceDriver.withInput(new Text("075508102118@163.gd|APP|1002.11.12"), values);
		reduceDriver.withOutput(new Text("Phone=13421812993,UserName=ace2"), new Text(""));
		reduceDriver.runTest();
	}
	
	public void testMR() throws IOException{
		
	}
	
	public static void main(String[] args) throws UnsupportedEncodingException {
		String url = "http://ping.pinyin.sogou.com/pingback.gif?h=5B7C9463CAC60460CC242CDA89489A03&mac=6C:25:B9:B8:9C:CA&v=8.0.0.8439&r=5282__5.1.0&id=Y10000001&passport=&type=predownload&ppversion=3.1.0.1972";
		System.out.println(findByRegex(url,"(mac)(=|:|_)(([0-9a-fA-F]{2})([:][0-9a-fA-F]{2}){5})",3));
		//System.out.println(decode64("123"));
		System.out.println(java.net.URLDecoder.decode("��", "utf-8"));
		test(1);
	}
	
	public static void test(int k){
		k  = k +2;
		System.out.println(k);
	}
	
	private static String findByRegex(String str, String regEx, int group) {
		String resultValue = null;
		if ((str == null) || (regEx == null) || ((regEx != null) && ("".equals(regEx.trim()))))
			return resultValue;

		Pattern p = Pattern.compile(regEx);
		Matcher m = p.matcher(str);

		boolean result = m.find();
		if (result) {
			resultValue = m.group(group);
		}
		return resultValue;
	}
	
	private static String decode64(String str) throws UnsupportedEncodingException {
		byte[] bt = null;
		try {
			sun.misc.BASE64Decoder decoder = new sun.misc.BASE64Decoder();
			bt = decoder.decodeBuffer(str);
		} catch (IOException e) {
			;
		}
		return bt == null ? str : new String(bt, "UTF-8");
	}
}

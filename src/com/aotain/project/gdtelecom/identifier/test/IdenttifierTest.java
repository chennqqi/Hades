package com.aotain.project.gdtelecom.identifier.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.aotain.project.gdtelecom.identifier.IdentifierReducer;
import com.aotain.project.gdtelecom.identifier.sz.IdentifierPMapper_Text;


public class IdenttifierTest {
	
	Configuration conf = new Configuration();
	MapDriver<LongWritable,Text,Text,Text> mapDriver;
	ReduceDriver<Text,Text,Text,Text> reduceDriver;

	@Before
	public void setUp() {
		IdentifierPMapper_Text mapper = new IdentifierPMapper_Text();
		mapDriver = MapDriver.newMapDriver(mapper);
		IdentifierReducer reducer = new IdentifierReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}
	
	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("075504420860@163.gd|6|61.141.116.49|59.151.86.72|16c7|50|www.ebery.com.cn|credit/card/firstFilter|www.rong360.com/credit/card/tianji?id_md5=359a3d10248653da268661947a7cece1|Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36|RONGID=88677e825e9f37208926d43142f4f70b; abclass=1477155001_5; __utmz=1477155001.utmcsr=360_utmcmd=oneboxxyk_utmccn=yinhang; PHPSESSID=tg1rc32213tdo7ip0g6hoqg6r4; historyCardIds=359a3d10248653da268661947a7cece1; cityDomain=shenzhen|1477155127|58|seachprov=å¹¿ä¸œ&Linkman=ace_ebery&id_md5=359a3d10248653da268661947a7cece1|shenzhen|20161023"));
		mapDriver.withInput(new LongWritable(), new Text("075504420860@163.gd|6|61.141.116.49|59.151.86.72|16c7|50|www.rong360.com|credit/card/firstFilter|www.rong360.com/credit/card/tianji?id_md5=359a3d10248653da268661947a7cece1|Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36|RONGID=88677e825e9f37208926d43142f4f70b; abclass=1477155001_5; __utmz=1477155001.utmcsr=360_utmcmd=oneboxxyk_utmccn=yinhang; PHPSESSID=tg1rc32213tdo7ip0g6hoqg6r4; historyCardIds=359a3d10248653da268661947a7cece1; cityDomain=shenzhen|1477155127|58|mobile=234&user_name=ace&id_md5=359a3d10248653da268661947a7cece1"));
		mapDriver.withOutput(new Text("Phone=13421812993,UserName=ace"), new Text(""));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() throws IOException{
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("2|QQ.COM"));
		reduceDriver.withInput(new Text("5|szlfh1288@163.gd|QQAccount,815972483,»ªÎª#P6"), values);
//		reduceDriver.withMultiOutput("haigou", new Text("Phone=13421812993,UserName=ace"), new Text(""));
		reduceDriver.withOutput(new Text("Phone=13421812993,UserName=ace"), new Text(""));
		reduceDriver.withOutput(new Text("Phone=13421812993,UserName=ace2"), new Text(""));
		reduceDriver.runTest();
	}
	

	public static void main(String[] args) {
		
		
		
	}
}

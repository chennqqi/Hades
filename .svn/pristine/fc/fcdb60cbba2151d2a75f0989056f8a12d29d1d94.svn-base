package dmpcommon.test;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.aotain.common.HFileConfigMgr;
import com.aotain.common.HFileConfigMgr.FieldItem;
import com.aotain.common.HfileConfig;
import com.aotain.common.ObjectSerializer;

import dmpcommon.KVTWMapper;
import dmpcommon.KVTWReducer;

public class TestTWMR {
	MapDriver<LongWritable,Text,Text,Text> mapDriver;
	ReduceDriver<Text,Text,Text,Text> reduceDriver;
	
	@Before
	public void setUp() throws Exception {
		KVTWMapper mapper = new KVTWMapper();
		mapDriver = MapDriver.newMapDriver(mapper);
		Configuration conf = mapDriver.getConfiguration();
		conf.set("date","20170320");
		
		HFileConfigMgr configMgr = new HFileConfigMgr("conf/tw_user_info.cfg");
		HfileConfig confHfile = configMgr.config;
		Map<String,Integer> mapPeriod = new HashMap<String,Integer>();
		for (FieldItem item : confHfile.getColumns()) {
			mapPeriod.put(item.FieldName, item.Period);
		}
		conf.set("KvPeriod", ObjectSerializer.serialize((Serializable) mapPeriod));
		
		String fieldsplit = confHfile.getFieldSplit();
		conf.set("fieldsplit",fieldsplit);

		KVTWReducer reducer = new KVTWReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		conf = reduceDriver.getConfiguration();
		conf.set("date", "20170320");
	}
	
	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("075508102118@163.gd,APP,1002.11.12,253:2_6:2_106:1_180:1,20170322,"));
		mapDriver.withInput(new LongWritable(), new Text("0331138@163.gd,APP,1005.12.265,51,1,20170322,"));
//		mapDriver.withInput(new LongWritable(), new Text("07550010376@163.gd,APP2,1012.1.112,1,2,20170310,"));
//		mapDriver.withInput(new LongWritable(), new Text("440300|075508102118@163.gd|113.90.233.254|msg.71.am|http://msg.71.am/core?t=5&a=7&ra=1&va=0&pf=2&p=22&p1=222&p2=3000&sdktp=1&c1=2&r=620614500&aid=620614500&u=869918021574267&pu=&v=8%2E1&krv=3%2E8%2E2&dt=&hu=-1&rn=1488286444&islocal=0&as=cbfd3b8126b189561f200c53f52ec4b3&ve=847fe0e92253127af89704103820c0b2&pe=&vfrm=&chl=&hcd|http://|android|3.8.2| | | |20170228205404| | |0| |0|QYPlayer/Android/3.8.2/nt_1|106.38.219.49|2c72|50|shenzhen|20170301"));
		mapDriver.withOutput(new Text(""), new Text(""));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() throws IOException{
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("6,0,20170322"));
		values.add(new Text("8,2,20170322"));
		reduceDriver.withInput(new Text("007550010376@163.gd,APP,1012.1.112"), values);
		reduceDriver.withOutput(new Text(""), new Text(""));
		reduceDriver.runTest();
	}
}

package com.aotain.project.gdtelecom.ua.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aotain.common.CommonFunction;
import com.aotain.project.gdtelecom.ua.mapping.DeviceParse;
import com.aotain.project.gdtelecom.ua.pojo.Device;
import com.aotain.project.gdtelecom.ua.util.Constant;


//公司深圳数据测试
public class UAPostParseMapperTest extends Mapper<LongWritable, Text, Text, Text> {
	protected Text keyout = new Text();
	protected Text valueout = new Text();
	
	
	protected  String inputSchema;
	protected String ua;
	protected String username;
	protected String domain;
	protected String outSplit;
	protected DeviceParse deviceParse = new DeviceParse();
	
	public UAPostParseMapperTest() {
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		outSplit = conf.get("OUT_SPLIT", ",");
		deviceParse.init(conf);
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] arr =value.toString().split("\\|");
		if(arr.length < 10 ) {
			return;
		}
		username = arr[1];
		ua = arr[9];
		domain = parseDomain(arr[6]);
		if(username !=null && !username.trim().equals("")) {
			parseUA(context);
		}
	}
	
	protected void parseUA(Context context) throws IOException, InterruptedException {
		if(null == ua || ua.trim().equals("") || CommonFunction.isMessyCode(ua)) {
			return;
		}
		
		// 解析UA
		Device device = deviceParse.getDevice(ua);
		
		if(device != null) {
			StringBuffer outkey = new StringBuffer();
			// checked ，输出名称
			if(null != device.getName()) {
				outkey.append(Constant.NAME_OUTPUT_CHECKED).append(username).append(outSplit)
				.append(device.getType()).append(outSplit)
				.append(device.getVendor()).append("#").append(device.getName());
			} 
			// uncheck , 输出型号
			else {
				String deviceType = null == device.getType()? "" : device.getType().toString();
				String deviceVendor = null == device.getVendor()? "" : device.getVendor();
				outkey.append(Constant.NAME_OUTPUT_UNCHECK).append(username).append(outSplit)
				.append(deviceType).append(outSplit)
				.append(deviceVendor).append("#").append(device.getModel());
			}
			keyout.set(outkey.toString());
			valueout.set(domain);
			context.write(keyout, valueout);
			
			// TODO 输出正常则,测试用
//			context.write(new Text(Constant.NAME_OUT_REGEX + device.getRegex()), new Text(domain));
		} 
		
		// TODO 测试用
		/*else {
			// 获取不到终端信息
			context.write(new Text(Constant.NAME_OUT_NODEVICE + ua), one);
		}
		*/
	}
	
	protected String parseDomain(String domain){
		if(domain == null) {
			return null;
		}
		String domains[] = domain.split("\\.");
		int domainLength = domains.length;
		String rootDomain = null;
		if(domainLength > 2) {
				 rootDomain = domain.substring(domain.indexOf(".")+1); 
		}else{
			     rootDomain = domain;
		}
//		rootDomain = rootDomain != null ? rootDomain : "null.com";
		return rootDomain;
	}

}

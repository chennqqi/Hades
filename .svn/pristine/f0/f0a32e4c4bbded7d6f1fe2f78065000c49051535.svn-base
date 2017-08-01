package com.aotain.project.gdtelecom.ua.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aotain.project.gdtelecom.ua.mapping.DeviceMapping;
import com.aotain.project.gdtelecom.ua.mapping.UAParse;
import com.aotain.project.gdtelecom.ua.pojo.Device;
import com.aotain.project.gdtelecom.ua.util.CalTimeUtil;

public class UAParseMapperText extends Mapper<NullWritable, Text, Text, Text> {

	protected  String inputSchema;
	protected  StructObjectInspector inputOI;
	protected String ua;
	protected String username;
	protected String domain;
	protected UAParse uaUtil;
	protected String outSplit;
	protected DeviceMapping mapping;
	protected final static IntWritable one = new IntWritable(1);

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		TypeInfo tfin = TypeInfoUtils.getTypeInfoFromTypeString(inputSchema);
		inputOI = (StructObjectInspector) OrcStruct.createObjectInspector(tfin);
		Configuration conf = context.getConfiguration();
		uaUtil = new UAParse();
		uaUtil.load("conf/ua_device.conf");
//		uaUtil.load(conf.get("CONFIG_FILE"), conf);
		outSplit = ",";
		mapping = new DeviceMapping();
		mapping.load("conf/devicecheck");
		CalTimeUtil.start("all");
	}
	
	public UAParseMapperText() {
		super();
	}

	@Override
	protected void cleanup(Mapper<NullWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		uaUtil.printRegexMap();
//		CalTimeUtil.end("all");
//		CalTimeUtil.print();
	}

	public UAParseMapperText(String inputSchema) {
		super();
		this.inputSchema = inputSchema;
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

	@Override
	protected void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
		
	}

	protected void parseUA(Context context) throws IOException, InterruptedException {
		if(null == ua || ua.trim().equals("")) {
			return;
		}
//		CalTimeUtil.start("getDevice");
		// 解析UA
		Device device = uaUtil.getDevice(ua);
//		CalTimeUtil.end("getDevice");
//		CalTimeUtil.start("out");
		if(device != null && null != device.getModel()) {
			// 从check表中匹配出相应的名称
			Device mappinged = mapping.mapping(device);
			
			StringBuffer outkey = new StringBuffer();
			// 不为空，输出名称
			if(null != mappinged) {
				outkey.append("checked_").append(username).append(outSplit)
				.append(mappinged.getType()).append(outSplit)
				.append(mappinged.getVendor()).append("#").append(mappinged.getName());
			} 
			// 否则输出型号
			else {
				String deviceType = null == device.getType()? "" : device.getType().toString();
				String deviceVendor = null == device.getVendor()? "" : device.getVendor();
				outkey.append("uncheck_").append(username).append(outSplit)
				.append(deviceType).append(outSplit)
				.append(deviceVendor).append("#").append(device.getModel());
			}
			context.write(new Text(outkey.toString()), new Text(domain));
			
			// TODO 输出正常则,测试用
//			context.write(new Text("rx_" + device.getRegex()), new Text(domain));
		} 
//		CalTimeUtil.end("out");
		
		// TODO 测试用
		/*else {
			// 获取不到终端信息
			context.write(new Text("no_" + ua), one);
		}
		*/
	}

}

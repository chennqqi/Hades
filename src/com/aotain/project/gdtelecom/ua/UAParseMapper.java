package com.aotain.project.gdtelecom.ua;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aotain.common.CommonFunction;
import com.aotain.project.gdtelecom.ua.mapping.DeviceParse;
import com.aotain.project.gdtelecom.ua.pojo.Device;
import com.aotain.project.gdtelecom.ua.util.Constant;

public class UAParseMapper extends Mapper<NullWritable, OrcStruct, Text, Text> {

	protected Text keyout = new Text();
	protected Text valueout = new Text();
	
	protected  String inputSchema;
	protected  StructObjectInspector inputOI;
	protected String ua;
	protected String username;
	protected String domain;
	protected String outSplit;
	protected List<Object> ilst;
	protected DeviceParse deviceParse = new DeviceParse();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		TypeInfo tfin = TypeInfoUtils.getTypeInfoFromTypeString(inputSchema);
		inputOI = (StructObjectInspector) OrcStruct.createObjectInspector(tfin);
		Configuration conf = context.getConfiguration();
		outSplit = conf.get("OUT_SPLIT", ",");
		deviceParse.init(conf);
	}
	
	public UAParseMapper() {
		super();
	}


	public UAParseMapper(String inputSchema) {
		super();
		this.inputSchema = inputSchema;
	}

	@Override
	protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
		
	}
	
	protected String getCol(int index) {
		Object obj = ilst.get(index);		
		return null == obj ? null : obj.toString();
	}
	
	protected String parseDomain(String domain){
		if("".equals(domain.trim())) {
			return domain;
		}
		String domains[] = domain.split("\\.");
		int domainLength = domains.length;
		String rootDomain = null;
		if(domainLength > 2) {
				 rootDomain = domain.substring(domain.indexOf(".")+1); 
		}else{
			     rootDomain = domain;
		}
		return rootDomain;
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
			
			// TODO 输出正则,测试用
			context.write(new Text(Constant.NAME_OUTPUT_REGEX + device.getRegex()), new Text(domain));
		} 
		
		// TODO 测试用
		/*else {
			// 获取不到终端信息
			context.write(new Text(Constant.NAME_OUT_NODEVICE + ua), one);
		}
		*/
	}

}

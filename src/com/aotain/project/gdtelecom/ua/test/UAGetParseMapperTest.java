package com.aotain.project.gdtelecom.ua.test;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.aotain.project.gdtelecom.ua.UAParseMapper;

//公司深圳数据测试
public class UAGetParseMapperTest extends UAParseMapper {

	final static String inputSchema_test = "struct<reaid:string,username:string,srcip:string,domain:string,url:string,refer:string,opersys:string,"
			+ "opersysver:string,browser:string,browserver:string,device:string,accesstime:bigint,cookie:string,keyword:string,urlclassid:string,"
			+ "referdomain:string,referclassid:string,ua:string,destinationip:string,sourceport:string,destinationpo:string>";
	
	public UAGetParseMapperTest() {
		super(inputSchema_test);
	}

	@Override
	protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
		List<Object> ilst = inputOI.getStructFieldsDataAsList(value);
		if (ilst.size() < 18) { 
			return;
		}
//		username = inputOI.getStructFieldData(value, inputOI.getStructFieldRef("username")).toString();
		
		Object obj = ilst.get(1);
		username = null == obj ? null : obj.toString();
		obj = ilst.get(3);
		domain = null == obj ? null : obj.toString();
		domain = parseDomain(domain);
		obj = ilst.get(17);
		ua = null == obj ? null : obj.toString();
		if(username !=null && !username.trim().equals("")) {
			parseUA(context);
		}
	}

}

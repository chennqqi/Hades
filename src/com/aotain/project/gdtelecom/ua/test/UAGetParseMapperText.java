package com.aotain.project.gdtelecom.ua.test;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class UAGetParseMapperText extends UAParseMapperText {

	final static String inputSchema = "struct<url:string,useragent:string,destinationip:string,destinationport:string,sourceport:string,link_info:string,cookie:string,"
			+ "contenttype:string,sca_catalog_type_id:string,sca_catalog_id:string,sca_classify_id:string,sca_site_id:string,sca_site_category_id:string,"
			+ "sca_ntlds:string,sca_domainname:string,sca_search_keywords:string,sca_web_keywords:string,sca_content_keywords:string,sca_title:string,"
			+ "sca_app_id:string,sca_app_name:string,sca_action_id:string,sca_action_name:string,sca_source_id:string,sca_source_name:string,"
			+ "sca_refer_id:string,sca_refer_name:string,account_nbr:string,protocol_type:string,sourceip:string,domain:string,visit_time:string,"
			+ "pack_len:string,pack_content:string,is_ip:int>";

	// 公司深圳数据测试用
	final static String inputSchema_test = "struct<reaid:string,username:string,srcip:string,domain:string,url:string,refer:string,opersys:string,"
			+ "opersysver:string,browser:string,browserver:string,device:string,accesstime:bigint,cookie:string,keyword:string,urlclassid:string,"
			+ "referdomain:string,referclassid:string,ua:string,destinationip:string,sourceport:string,destinationpo:string>";
	
	public UAGetParseMapperText() {
//		super(inputSchema);// TODO 正式环境时
		super(inputSchema_test);
	}

	@Override
	protected void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
//		CalTimeUtil.start("map");
		String[] ilst = value.toString().split("	");
//		List<Object> ilst = inputOI.getStructFieldsDataAsList(value);
//		if (ilst.size() != 35) { // TODO正式环境
//			return;
//		}
		if (ilst.length < 18) { // TODO公司深圳数据环境
			return;
		}
		Object obj = ilst[1];
		username = null == obj ? null : obj.toString();
//		obj = ilst.get(27);// TODO正式环境
		obj = ilst[17];// TODO公司深圳数据环境
//		CalTimeUtil.start("domain");
		domain = parseDomain(ilst[3]);
//		CalTimeUtil.end("domain");
		ua = null == obj ? null : obj.toString();
		if(username !=null && !username.trim().equals("")) {
			parseUA(context);
		}
//		CalTimeUtil.end("map");
	}

}

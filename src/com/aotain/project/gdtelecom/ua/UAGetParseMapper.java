package com.aotain.project.gdtelecom.ua;

import java.io.IOException;

import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.io.NullWritable;

public class UAGetParseMapper extends UAParseMapper {

	final static String inputSchema = "struct<url:string,useragent:string,destinationip:string,destinationport:string,sourceport:string,link_info:string,cookie:string,"
			+ "contenttype:string,sca_catalog_type_id:string,sca_catalog_id:string,sca_classify_id:string,sca_site_id:string,sca_site_category_id:string,"
			+ "sca_ntlds:string,sca_domainname:string,sca_search_keywords:string,sca_web_keywords:string,sca_content_keywords:string,sca_title:string,"
			+ "sca_app_id:string,sca_app_name:string,sca_action_id:string,sca_action_name:string,sca_source_id:string,sca_source_name:string,"
			+ "sca_refer_id:string,sca_refer_name:string,account_nbr:string,protocol_type:string,sourceip:string,domain:string,visit_time:string,"
			+ "pack_len:string,pack_content:string,is_ip:int>";

	public UAGetParseMapper() {
		super(inputSchema);
	}

	@Override
	protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
		ilst = inputOI.getStructFieldsDataAsList(value);
		if (ilst.size() < 31) {
			return;
		}
		username = getCol(27);
		domain = parseDomain(getCol(30));
		ua = getCol(1);
		if(username !=null && !username.trim().equals("")) {
			parseUA(context);
		}
	}

}

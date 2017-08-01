package com.aotain.project.gdtelecom.ua;

import java.io.IOException;

import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.io.NullWritable;

public class UAPostParseMapper extends UAParseMapper {

	final static String inputSchema = "struct<url:string,useragent:string,destinationip:string,destinationport:string,sourceport:string,link_info:string,cookie:string,"
			+ "contenttype:string,account_nbr:string,protocol_type:string,sourceip:string,domain:string,visit_time:string,pack_len:string,pack_content:string,"
			+ "is_ip:int,content_type:string>";
	
	public UAPostParseMapper() {
		super(inputSchema);
	}
	
	@Override
	protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
		ilst = inputOI.getStructFieldsDataAsList(value);
		if (ilst.size() < 10) {
			return;
		}
		
		username  = getCol(8);
		domain = parseDomain(getCol(11));
		ua = getCol(1);
		if(username !=null && !username.trim().equals("")) {
			parseUA(context);
		}
	}
}

package com.aotain.project.gdtelecom.identifier;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * �㶫�����з�����post
 * @author Administrator
 *
 */
public class IdentifierPMapper_yanfa extends IdentifierMapper<NullWritable, OrcStruct> {

	private static final String inputSchema = "struct<url:string,useragent:string,destinationip:string,destinationport:string,sourceport:string,"
			+ "link_info:string,cookie:string,contenttype:string,sca_catalog_type_id:string,sca_catalog_id:string,sca_classify_id:string,sca_site_id:string,"
			+ "sca_site_category_id:string,sca_ntlds:string,sca_domainname:string,sca_search_keywords:string,sca_web_keywords:string,"
			+ "sca_content_keywords:string,sca_title:string,sca_app_id:string,sca_app_name:string,sca_action_id:string,sca_action_name:string,"
			+ "sca_source_id:string,sca_source_name:string,sca_refer_id:string,sca_refer_name:string,account_nbr:string,protocol_type:string,"
			+ "sourceip:string,domain:string,visit_time:string,pack_len:string,pack_content:string>";

	private StructObjectInspector inputOI;

	@Override
	protected void setup(Mapper<NullWritable, OrcStruct, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		TypeInfo tfin = TypeInfoUtils.getTypeInfoFromTypeString(inputSchema);
		inputOI = (StructObjectInspector) OrcStruct.createObjectInspector(tfin);
	}

	@Override
	protected void map(NullWritable key, OrcStruct value, Mapper<NullWritable, OrcStruct, Text, Text>.Context context)
			throws IOException, InterruptedException {
		try {
			List<Object> ilst = inputOI.getStructFieldsDataAsList(value);
			if (ilst.size() < 34) {
				return;
			}
			super.reset();
			sUserName = getCol(ilst, 27);
			if (sUserName == null || sUserName.length()==0) {
				return;
			}

			url = getCol(ilst, 0);
			cookie = getCol(ilst, 6);
			domain = getCol(ilst, 30);
			ua = getCol(ilst, 1);
			handleBase();
			
			pack_contnt = getCol(ilst, 33);
			if (pack_contnt != null) {
				if (pack_contnt.contains("%")) {
					pack_contnt = pack_contnt.replaceAll("%(?![0-9a-fA-F]{2})", "%25");
					pack_contnt = java.net.URLDecoder.decode(pack_contnt, "utf-8");
				}
				pack_contnt = pack_contnt.replace("\"", "").toLowerCase();
			}
			
			String timestamp = getCol(ilst, 31);
			long hour = 10;
			try {
				hour = Long.parseLong(timestamp.substring(8, 10));
			} catch (Exception e) {
			}

			
			int weight = 1;
			if ((hour >= 21 && hour <= 23) || (hour >= 0 && hour <= 7)) {
				weight = 2;
			}
			weight_device = weight;
			
			imei(weight, context);
			device( context);
			app(weight,context);
			mail(weight, context);
			mac_terminal(weight, context);
			phone(weight, context);
			imsi(weight, context);
			qq(weight, context);
			idfa(weight, context);

		} catch (Exception e) {
			;
		}

	}


}
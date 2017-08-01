package com.aotain.project.gdtelecom.identifier.test;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aotain.project.gdtelecom.identifier.IdentifierMapper;

/**
 * 现场环境test-用户标识解析post数据Map
 * @author Liangsj
 *
 */
public class IdentifierPMapper_gdText extends IdentifierMapper<LongWritable, Text> {

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] items = value.toString().split("\\|", -1);
		if (items.length < 15) {
			return;
		}
		try {
			super.reset();
			sUserName = items[8];
			if (sUserName == null || sUserName.length()==0) {
				return;
			}
			
			url = items[0].trim();
			cookie = items[6].trim();
			domain = items[11].trim();
			ua = items[1];
			ip=items[2];
			port=items[3];
			
			handleBase();
			
			pack_contnt = items[14];
			if (pack_contnt != null) {
				if (pack_contnt.contains("%")) {
					pack_contnt = pack_contnt.replaceAll("%(?![0-9a-fA-F]{2})", "%25");
					pack_contnt = java.net.URLDecoder.decode(pack_contnt, "utf-8");
				}
				pack_contnt = pack_contnt.replace("\"", "").toLowerCase();
			}
			
			String timestamp = items[12];
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
			device(context);
			app(weight,context);
			mail(weight, context);
			mac_terminal(weight, context);
			phone(weight, context);
			imsi(weight, context);
			qq(weight, context);
			idfa(weight, context);
			
		} catch (Exception e) {
		}

	}

}

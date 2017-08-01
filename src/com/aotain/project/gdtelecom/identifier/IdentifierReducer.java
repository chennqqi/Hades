package com.aotain.project.gdtelecom.identifier;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.aotain.project.gdtelecom.identifier.enums.Attcode;
import com.aotain.project.gdtelecom.ua.util.Constant;

public class IdentifierReducer extends Reducer<Text, Text, Text, Text> {

	private Text outkey = new Text();
	private Text outvalue = new Text("");
	private String date;
	private static final String OUT_SPLIT = ",";
	MultipleOutputs<Text, Text> collector = null;
	protected String  nameOutput = Constant.NAME_OUTPUT_NORMAL;
	
	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		date = context.getConfiguration().get("date");
		collector = new MultipleOutputs<Text, Text>(context);
	}

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String keystr = key.toString();
		
		// 输出数据源
		if(keystr.startsWith(Constant.NAME_OUTPUT_SOURCE)) {
			outkey.set(keystr.substring(Constant.NAME_OUTPUT_SOURCE.length()));
			nameOutput = Constant.NAME_OUTPUT_SOURCE;
			collector.write(nameOutput, outkey, outvalue, nameOutput+ "/" + nameOutput);
			return;
		} 
		
		// 统计频次并输出
		Set<Object> rt1 = new HashSet<Object>();
		Set<Object> rt2 = new HashSet<Object>();
		Set<Object> rt3 = new HashSet<Object>();
		String[] vkey = key.toString().split("\\|", -1);
		String attcode =vkey[0];
		for (Text t : values) {
			String[] splitvs = t.toString().trim().split("\\|", -1);
			if ("1".equals(splitvs[0])) {
				rt1.add(splitvs[1]);
			} else if ("2".equals(splitvs[0])) {
				rt2.add(splitvs[1]);
			} else if ("3".equals(splitvs[0])) {
				rt3.add(splitvs[1]);
			}
		}
		StringBuffer sb = new StringBuffer();
		sb.append(vkey[1]).append(OUT_SPLIT)
			.append(attcode).append(OUT_SPLIT)
			.append(vkey[2]).append(OUT_SPLIT)
			.append(rt1.size() + rt2.size() * 2 + rt3.size() * 3).append(OUT_SPLIT)
			.append(date).append(OUT_SPLIT);
		outkey.set(sb.toString());
//		context.write(outkey, outvalue);
		nameOutput = Constant.NAME_OUTPUT_NORMAL;
		collector.write(nameOutput, outkey, outvalue, nameOutput+ "/" + nameOutput);
	}

}

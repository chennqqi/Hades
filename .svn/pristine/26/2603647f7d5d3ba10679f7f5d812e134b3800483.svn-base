package com.aotain.project.gdtelecom.ua;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.aotain.project.gdtelecom.ua.util.Constant;

public class UAParseReducer extends Reducer<Text, Text, Text, Text>{
	
	private IntWritable count = new IntWritable();
	private String outSplit;
	private String date;
	protected Text keyout = new Text();
	protected Text valueout = new Text("");
	protected String  nameOutput = Constant.NAME_OUTPUT_CHECKED;
	
	MultipleOutputs<Text, Text> collector = null;
	
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		collector.close();
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		outSplit = conf.get("OUT_SPLIT", ",");
		date = conf.get("DATE", "20170101");
		collector = new MultipleOutputs<Text, Text>(context);
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		// ∂‘values «Ûcount distinct
		Set<Text> set = new HashSet<Text>();
		for (Text val : values) {
			set.add(val);
		}
		count.set(set.size());
		
		String keystring  =key.toString();
		StringBuffer sb = new StringBuffer();
		sb.append(keystring).append(outSplit).append(count).append(outSplit).append(date).append(outSplit);
		
		if(keystring.startsWith(Constant.NAME_OUTPUT_CHECKED)) {
			keyout.set(sb.substring(Constant.NAME_OUTPUT_CHECKED.length()));
			nameOutput = Constant.NAME_OUTPUT_CHECKED;
			
		} else if(keystring.startsWith(Constant.NAME_OUTPUT_UNCHECK)) {
			keyout.set(sb.substring(Constant.NAME_OUTPUT_UNCHECK.length()));
			nameOutput = Constant.NAME_OUTPUT_UNCHECK;
		}
		
		/*// TODO ≤‚ ‘
		else if(keystring.startsWith(Constant.NAME_OUTPUT_NODEVICE)) {
			nameOutput = Constant.NAME_OUTPUT_NODEVICE;
		} */
		else if(keystring.startsWith(Constant.NAME_OUTPUT_REGEX)) {
			nameOutput = Constant.NAME_OUTPUT_REGEX;
		}
		collector.write(nameOutput, keyout, valueout, nameOutput+ "/" + nameOutput);
	}

}





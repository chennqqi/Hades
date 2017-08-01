package com.aotain.project.npcheck;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.aotain.common.CommonFunction;

public class MBCheckReducer extends Reducer<Text, Text, Text, NullWritable>{
	protected void reduce(Text rkey, java.lang.Iterable<Text> rvalue, org.apache.hadoop.mapreduce.Reducer<Text,Text,Text,NullWritable>.Context context) throws java.io.IOException ,InterruptedException {
		String key = rkey.toString()+",";
		//同一账号设备个数
		Set<String> deviceSet = new HashSet<String>();
		for (Text text : rvalue) {
			deviceSet.add(text.toString());
		}
		int deviceCount = deviceSet.size();
		String out =key + deviceCount+",0"+",www,";
		context.write(new Text(out), null);
	};

	
}

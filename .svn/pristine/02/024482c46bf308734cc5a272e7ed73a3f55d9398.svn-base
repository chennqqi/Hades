package com.aotain.project.gdtelecom.identifier;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.aotain.project.gdtelecom.identifier.enums.Attcode;
import com.aotain.project.gdtelecom.ua.util.Constant;

public class IdentifierReducerTest extends Reducer<Text, Text, Text, Text> {

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
		
		int sum = 0;
		for(Text val : values){
			sum += Integer.parseInt(val.toString());
		}
		
		outkey.set(key);
		outvalue.set(sum + "");
		nameOutput = Constant.NAME_OUTPUT_NORMAL;
		collector.write(nameOutput, outkey, outvalue, nameOutput+ "/" + nameOutput);
	}

}

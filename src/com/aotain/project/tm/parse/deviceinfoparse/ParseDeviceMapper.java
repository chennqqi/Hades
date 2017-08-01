package com.aotain.project.tm.parse.deviceinfoparse;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.aotain.project.tm.parse.common.conf.BrandConf;

public class ParseDeviceMapper extends Mapper<Object, Text, Text, NullWritable> {

	private DeviceParser deviceParser;
	private Text keyOut = new Text();
	private NullWritable valOut = NullWritable.get();
	private MultipleOutputs<Text, NullWritable> collector = null;

	@Override
	protected void cleanup(Mapper<Object, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		collector.close();
		super.cleanup(context);
	}
	
	@Override
	protected void setup(Mapper<Object, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		collector = new MultipleOutputs<Text, NullWritable>(context);
		String brandConfPath = conf.get("BRAND_CONF");
		BrandConf brandConf = new BrandConf();
		brandConf.load(brandConfPath, conf);
		deviceParser = new DeviceParser(brandConf);
		super.setup(context);
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		deviceParser.parse(value.toString());
		if (deviceParser.isPriceLegal() && !deviceParser.isPhoneBefore2010()) {
			Set<String> models = deviceParser.getModels();
			if(models == null || models.isEmpty()){
				keyOut.set(value.toString());
				collector.write(ParseDeviceDriver.NAME_OUT_NO_MODEL, keyOut, valOut, ParseDeviceDriver.NAME_OUT_NO_MODEL + "/"+ParseDeviceDriver.NAME_OUT_NO_MODEL);
				return;
			}
			for (String m : models) {
				keyOut.set(m + "|" + deviceParser.getName() + "|" + deviceParser.getBrand() + "|"
						+ deviceParser.getType()+ "|");
				collector.write(ParseDeviceDriver.NAME_OUT_MODEL, keyOut, valOut, ParseDeviceDriver.NAME_OUT_MODEL + "/"+ParseDeviceDriver.NAME_OUT_MODEL);
			}
			
		}
	}
}

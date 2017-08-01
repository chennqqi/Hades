package com.aotain.project.tm.parse.deviceinfoparse;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ParseDeviceDriver extends Configured implements Tool {
	
	public static final String  NAME_OUT_MODEL = "model";
	public static final String  NAME_OUT_NO_MODEL = "nomodel";
	
    public static void main(String[] args)throws Exception{
        int exitcode = ToolRunner.run(new ParseDeviceDriver(), args);
        System.exit(exitcode);                  
   }
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		String input = conf.get("input");
		String output = conf.get("output");

		if (!check(input, "input") || !check(output, "output")) {
			return 1;
		}

		// 如果输出目录已存在，需要删除
		FileSystem fsTarget = FileSystem.get(URI.create(output), conf);
		Path pathTarget = new Path(output);
		if (fsTarget.exists(pathTarget)) {
			fsTarget.delete(pathTarget, true);
		}

		Job job = Job.getInstance(conf);
		job.setJobName("ParseDevice MR");
		job.setJarByClass(getClass());
		job.setMapperClass(ParseDeviceMapper.class);
		job.setMapOutputKeyClass(Text.class);

		String[] inputpaths = getPathStrings(input, conf); // 以逗号分隔的多个路径
		if (inputpaths == null || inputpaths.length == 0) {
			System.out.println("输入路径没有，程序退出：" + input);
			return 1;
		}
		for (String path : inputpaths) {
			FileInputFormat.addInputPath(job, new Path(path));
			System.out.println("input:");
			System.out.println(path);
		}
		FileOutputFormat.setOutputPath(job, new Path(output));
		MultipleOutputs.addNamedOutput(job, NAME_OUT_MODEL, TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, NAME_OUT_NO_MODEL, TextOutputFormat.class, Text.class, NullWritable.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);//记录输出时才真正创建文件,避免生成空文件
		return job.waitForCompletion(true) ? 0 : 1;
	}

	private static boolean check(final String value, final String key) {
		System.out.println(key + ":" + value);
		if (value == null) {
			System.err.println("no " + key + " param, Usage: -D " + key + "=xxx");
			System.out.println(
					"Usage: hadoop jar  <jarfile> WordDriver -D input=xxx -D output=xxx -D date=20170215 -D mapreduce.job.reduces=4");
			return false;
		}
		return true;
	}

	private String[] getPathStrings(String commaSeparatedPaths, Configuration conf) {
		List<String> pathStrings = new ArrayList<String>();
		String[] arr = commaSeparatedPaths.split(",");
		for (String p : arr) {
			if (hdfsPathExists(p, conf)) {
				pathStrings.add(p);
			}
		}
		return pathStrings.toArray(new String[0]);
	}

	private boolean hdfsPathExists(String path, Configuration conf) {
		if (null == path || path.trim().equals("")) {
			return false;
		}
		try {
			Path pathTarget = new Path(path);
			FileSystem fsTarget = FileSystem.get(URI.create(path), conf);
			return (fsTarget.exists(pathTarget));
		} catch (IOException e) {
			return false;
		}

	}
}


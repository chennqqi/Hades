package com.aotain.push;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.mapreduce.LzoTextInputFormat;

public class RadiusIntoHbase extends Configured implements Tool{

	public static void main(String[] args) throws Exception {

		int exitcode = ToolRunner.run(new RadiusIntoHbase(), args);
		System.exit(exitcode); 
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.16.1.40");
		conf.set(TableOutputFormat.OUTPUT_TABLE, "PUSH_RADIUS");
		conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive",true);
		conf.set("dfs.socket.timeout", "180000");

		String inputPostPath = args[0];
		String remark = args[1];

		//如果输入路径不存在则退出
		FileSystem fsSource = FileSystem.get(URI.create(inputPostPath), conf);
		Path pathSource = new Path(inputPostPath);
		if(!fsSource.exists(pathSource)) {
			return 0;
		}

		Job job = new Job(conf, RadiusIntoHbase.class.getSimpleName());

		//当打包成jar运行时，必须有以下2行代码
		TableMapReduceUtil.addDependencyJars(job);
//		job.setJarByClass(RadiusIntoHbase.class);
		job.setJarByClass(getClass());//如果用Tool这种方式，得到类就需要这样弄了

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReduce.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

//		job.setInputFormatClass(TextInputFormat.class);
		job.setInputFormatClass(LzoTextInputFormat.class);
		//这里要用TableOutputFormat
		job.setOutputFormatClass(TableOutputFormat.class);

		//		FileInputFormat.setInputPaths(job, "hdfs://nameservice1:8020/user/hive/warehouse/broadband.db/ipsradius/shenzhen/20150614");
		FileInputFormat.setInputPaths(job, inputPostPath);
		return job.waitForCompletion(true)?0:1;
	}


	static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		Text v2 = new Text();
		@Override
		protected void map(LongWritable key, Text value,
				org.apache.hadoop.mapreduce.Mapper.Context context)
						throws IOException, InterruptedException {
			String line = value.toString();
			if(line != null && !"".equals(line) ) {
				String[] splits = line.split(",");
				if(splits.length>=9) {
					String rowkey = splits[0]+"_" +splits[1];
					v2.set(rowkey+","+line);
					context.write(key, v2);
				}
			}
		}
	}

	static class MyReduce extends TableReducer<LongWritable, Text, NullWritable> {

		@Override
		protected void reduce(LongWritable key, Iterable<Text> v2s,
				Context context)
						throws IOException, InterruptedException {
			for(Text v2:v2s) {
				String[] splits = v2.toString().split(",");

				String rowKey = splits[0];
				if(rowKey != null && !"".equals(rowKey)) {

					Put put = new Put(rowKey.getBytes());

					put.add("cf".getBytes(), "HAPPENTIME".getBytes(), splits[1].getBytes());
					put.add("cf".getBytes(), "USERNAME".getBytes(), splits[2].getBytes());
					put.add("cf".getBytes(), "STRIP".getBytes(), splits[3].getBytes());
					put.add("cf".getBytes(), "TYPE".getBytes(), splits[4].getBytes());
					put.add("cf".getBytes(), "IFNAT".getBytes(), splits[5].getBytes());
					put.add("cf".getBytes(), "STARTPORT".getBytes(), splits[6].getBytes());
					put.add("cf".getBytes(), "ENDPORT".getBytes(), splits[7].getBytes());
					put.add("cf".getBytes(), "PRIVATEIP".getBytes(), splits[8].getBytes());
					put.add("cf".getBytes(), "AREAID".getBytes(), splits[9].getBytes());
					//					put.add("cf".getBytes(), "rxbytes4gflg".getBytes(), splits[10].getBytes());
					//					put.add("cf".getBytes(), "txbytes4gflg".getBytes(), splits[11].getBytes());
					//					put.add("cf".getBytes(), "sessionid".getBytes(), splits[12].getBytes());
					//					put.add("cf".getBytes(), "sessiontime".getBytes(), splits[13].getBytes());
					//					put.add("cf".getBytes(), "eventtime".getBytes(), splits[14].getBytes());
					context.write(NullWritable.get(), put);
				}
			}
		}
	}

	

}

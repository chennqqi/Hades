package com.aotain.dim;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DeviceAttMapper extends Mapper<LongWritable,Text,Text,Text> {
	
	private String fieldSplit;
	private String keyValueSplit;
	private String[] columns;
	private String[] keyColumns;
	private String newSplit;
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		fieldSplit = conf.get("FIELD_SPLIT");
		keyValueSplit = conf.get("KEY_VALUE_SPLIT");
		columns = null != conf.get("COLUMNS") ? conf.get("COLUMNS").split(",") : null;
		keyColumns =  null != conf.get("KEY_COLUMNS") ? conf.get("KEY_COLUMNS").split(",") : null;
		newSplit = conf.get("NEW_SPLIT");
		super.setup(context);
	}
	
	@Override
	// 品牌$华为,型号$C8817E
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] datas = value.toString().split(fieldSplit);
		StringBuffer outKey = new StringBuffer(); 
		
		// 获取key值
		for(String keycol : keyColumns) {
			for(String data : datas) {
				String[] dataSplit = data.split(keyValueSplit);
				if(dataSplit.length >=2 && keycol.equals(dataSplit[0])) {
					outKey.append(dataSplit[1]).append(newSplit);
					break;
				}
			}
		}
		if(outKey.length() == 0) {
			System.out.println("no outKey:" + value);
			return;
		}
		Text outKeyText = new Text(outKey.substring(0, outKey.length() -1 ));
		
		// 输出 key value
		for(String col : columns) {
			for(String data : datas) {
				String[] dataSplit = data.split(keyValueSplit);
				if(dataSplit.length >=2 && col.equals(dataSplit[0])) {
					String data_key = dataSplit[0];
					String data_value = dataSplit[1];
					if("价格".equals(data_key)){
						try {
							float price = Float.parseFloat(data_value);
							if (price >= 0 && price < 500)
								data_value = "0~499元";
							else if (price >= 500 && price < 1000)
								data_value = "500~999元";
							else if (price >= 1000 && price < 1500)
								data_value = "1000~1499元";
							else if (price >= 1500 && price < 2000)
								data_value = "1500~1999元";
							else if (price >= 2000 && price < 2500)
								data_value = "2000~2499元";
							else if (price >= 2500 && price < 3000)
								data_value = "2500~2999元";
							else if (price >= 3000 && price < 3500)
								data_value = "3000~3499元";
							else if (price >= 3500)
								data_value = "3500元以上";
						} catch (Exception e) {
						}
					} else if ("主屏尺寸".equals(data_key)) {
						float screen = 0;
						try {
							screen = Float.parseFloat(data_value.replace("英寸", ""));
							if (screen >= 10.1)
								data_value = "11英寸及以上 ";
							else if (screen > 9.7 && screen <= 10.1)
								data_value = "10.1英寸";
							else if (screen > 7.9 && screen <= 9.7)
								data_value = "8-9.7英寸";
							else if (screen > 7.0 && screen <= 7.9)
								data_value = " 7.9英寸";
							else if (screen > 6.0 && screen <= 7.0)
								data_value = "7英寸 ";
							else if (screen <= 6.0)
								data_value = "6英寸及以下";
						} catch (Exception e) {
						}
					}
					context.write(outKeyText, new Text(data_key + newSplit + data_value));
					break;
				}
			}
		}
	}
	
}














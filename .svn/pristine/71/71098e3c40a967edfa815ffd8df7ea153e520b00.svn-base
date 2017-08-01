package com.aotain.dim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DeviceAttReducer extends Reducer<Text, Text, Text, Text>{
	
	private String[] columns;// 输出的列
	private String[] columnValues;// 输出列值
	private String newSplit;// 输出分隔符
	private String typeValue;// "类型"字段的值
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		if(null == conf.get("COLUMNS")) {
			throw new RuntimeException("输出列名不能为空");
		}
		columns =  conf.get("COLUMNS").split(",");
		newSplit = conf.get("NEW_SPLIT");
		typeValue = conf.get("TYPE_VALUE");
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		columnValues = new String[columns.length];
		//value1:华为C8817E（电信4G）, 价格$499
		//value2:华为C8817E（电信4G）, 品牌$华为
		//输出字段：型号,名称,品牌,类型,价格,上市日期,主屏尺寸
		List<String> valueTemp = new ArrayList<String>();
		for(Text value : values) {
			valueTemp.add(value.toString());
		}
		StringBuffer outValue = new StringBuffer();
		int name_index = -1;
		int model_index = -1;
		for(int i=0; i < columns.length; i++) {
			columnValues[i]="";
			String col = columns[i];
			if(col.equals("类型")){
				columnValues[i] = typeValue;
			} else {
				for(String value : valueTemp) {
					String[] valueSplit = value.toString().split(newSplit);
					if(valueSplit.length>=2 && col.equals(valueSplit[0])) {
						columnValues[i] = valueSplit[1];
						break;
					}
				}
			}
			if(col.equals("名称")) {
				name_index = i;
			} else if (col.equals("型号")) {
				model_index = i;
			}
		}
		
		if(model_index !=-1 &&   name_index != -1 && (columnValues[model_index] == null || columnValues[model_index].equals(""))) {
			String model = parseTelTypeFromName(columnValues[name_index]);
			if(!model.equals("")) {
				columnValues[model_index] = model;
			}
		}
		for(String col : columnValues) {
			outValue.append(col).append(newSplit);
		}
		context.write(new Text(outValue.toString().substring(0, outValue.length() -1)), new Text());
	}
	
	public String parseTelTypeFromName(String name){
		//Moto RAZR V锋芒（MT887/移动版）
		String regex1 = "（([-a-zA-Z\\d\\s\\+]*)\\/";// 取（到/部分
		String regex2 = "([-a-zA-Z\\d\\s\\+]+)[（]";
		String regex3 = "([-a-zA-Z\\d\\s\\+]+)";
		String result = find(name, regex1);
		if(!isModel(result)) {
			result = find(name, regex2);
		} 
		if(!isModel(result)) {
			result = find(name, regex3);
		}
		if(!isModel(result)) {
			result = "";
		}
		return result.toUpperCase();
		
	}
	
	private boolean isModel(String model) {
		return model != null && !model.contains("GB") && !model.contains("4G") && !model.contains("3G") && !model.contains("2G");
	}
	
	private String find(String str, String regex) {
		Pattern pat = Pattern.compile(regex);
		Matcher mat = pat.matcher(str);
		if(mat.find()){
			return mat.group(1);
		}
		return null;
	}

}





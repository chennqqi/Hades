package com.aotain.common;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import jline.internal.InputStreamReader;

public class PropertiesUtil {
	
	private Properties prop = new Properties();

	public PropertiesUtil(String file) {
		InputStreamReader in = null;
		try {
			in = new InputStreamReader(new FileInputStream(file), "utf-8");
			prop.load(in);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("读取配置文件异常", e);
		} finally {
			if(in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public  String getString(String key) {
		return prop.getProperty(key);
	}
	
	public String getString(String key, String defaultValue) {
		return prop.getProperty(key, defaultValue);
	}
	
	public static void main(String[] args) {
		PropertiesUtil pro = new PropertiesUtil("conf/device.properties");
		System.out.println(pro.getString("COLUMNS"));
	}
}

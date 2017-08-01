package com.aotain.project.gdtelecom.identifier.adapter;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.aotain.common.CommonFunction;
import com.aotain.project.gdtelecom.ua.pojo.Device;
import com.aotain.project.gdtelecom.ua.pojo.DeviceType;

public class IMEIAdapter {

	/**
	 * 映射配置数据，key->imei前六位   value->Device
	 */
	private Map<String, Device> deviceMappings = new HashMap<String, Device>();
	
	/**
	 * 匹配
	 * @param imei 
	 * @return Device 若匹配得上，返回新的Device对象(check表的名称、品牌、类型)，否则返回null
	 */
	public Device mapping(String imei) {
		if(null == imei) {
			return null;
		}
		String imei_6 = imei.substring(0,6);
		return deviceMappings.get(imei_6);
	}
	
	/**
	 * 从本地加载配置
	 * @param checkFile check配置文件路径
	 */
	public void load(String checkFile) {
		InputStream checkin = null;
		try {
			checkin = new FileInputStream(checkFile);
			loadMapping(checkin);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new RuntimeException("加载配置异常", e);
		} finally{
			close(checkin, null);
		}
	}
	
	/**
	 * 从HDFS加载配置
	 * @param hdfsMappingFile hdfs check配置文件路径
	 * @param conf
	 */
	public void load(String hdfsCheckPath, Configuration conf) {
		InputStream checkin = null;
		try {
			System.out.println("加载映射路径:hdfs_path=" + hdfsCheckPath);
			FileSystem fs = FileSystem.get(conf);
			Path  path = new Path(hdfsCheckPath);
			if(fs.exists(path)) {
				for(FileStatus  filestatus : fs.listStatus(path)) {
					System.out.println("加载映射数据:file=" + filestatus.getPath());
					checkin = fs.open(filestatus.getPath());
					loadMapping(checkin);
					close(checkin, null);
				}
			}
			System.out.println("映射数据加载完成,size:" + deviceMappings.size());
			if(deviceMappings.size() == 0) {
				throw new RuntimeException("imei-device配置数据为空，请检查,hdfs_path="+ hdfsCheckPath);
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("加载配置异常", e);
		}finally{
			close(checkin, null);
		}
	}
	
	
	/**
	 * 加载mapping配置
	 * @param in
	 */
	public void loadMapping(InputStream in) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(in, "utf-8"));
			String line = null;
			while((line=br.readLine())!=null) {
				String[] arr = line.split("\\|");
				if(arr.length < 6) {
					continue;
				}
				String key = arr[0].trim();// imei前六位
				String name = arr[1].trim();
				String vendor = arr[2].trim().toUpperCase();
				String type = arr[3].trim();
				String isvalid = arr[5].trim();
				if(!CommonFunction.isNull(name) && isvalid.trim().equals("1")) {
					Device device = new Device();
					device.setName(name);
					device.setVendor(vendor);
					device.setType(DeviceType.nameOf(type));
					deviceMappings.put(key, device);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			close(null, br);
		}
	}
	
	public void close(InputStream in, Reader reader) {
		try {
			if(in != null) {
				in.close();
			}
			if(reader != null) {
				reader.close();
			}
			} catch (IOException e) {
		}
	}
	
	public static void main(String[] args) {
		IMEIAdapter mapping = new IMEIAdapter();
		mapping.load("conf/imei_device.conf");
		System.out.println(mapping.deviceMappings.size());
		Device result =mapping.mapping("35224834324");
		System.out.println(result);
	}
}

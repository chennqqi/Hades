package com.aotain.project.gdtelecom.ua.mapping;

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

/**
 * 匹配型号
 * 根据型号，查询check表，匹配出终端名称
 * @author Liangsj
 *
 */
public class DeviceMapping {

	/**
	 * 映射配置数据，key->ua中的 型号   value->Device
	 */
	private Map<String, Device> deviceMappings = new HashMap<String, Device>();
	
	
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
				throw new RuntimeException("check配置数据为空，请检查,hdfs_path="+ hdfsCheckPath);
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
				if(arr.length < 4) {
					continue;
				}
				String key = arr[0].trim() +  arr[3].trim();// 型号 + 类型
				String name = arr[2].trim();
				String vendor = arr[1].trim();
				String type = arr[3].trim();
				if(!CommonFunction.isNull(name)) {
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
	
	/**
	 * 匹配,按型号+类型，如果类型为unknow,先匹配 mobile 再pad 再box 最后tv
	 * @return Device 若匹配得上，返回新的Device对象(check表的名称、品牌、类型)，否则返回null
	 */
	public Device mapping(Device devicekey) {
		Device result = null;
		String model = devicekey.getModel();
		DeviceType type = devicekey.getType() == null ? DeviceType.UNKNOWN : devicekey.getType();
		String key = model + type;
		if(type != DeviceType.UNKNOWN) {
			result = deviceMappings.get(key);
		} else {
			key = model + DeviceType.MOBILE;
			result = deviceMappings.get(key);
			if(null == result) {
				key = model + DeviceType.PAD;
				result = deviceMappings.get(key);
				 if(null == result) {
					key = model + DeviceType.BOX;
					result = deviceMappings.get(key);
					if(null == result) {
						key = model + DeviceType.TV;
						result = deviceMappings.get(key);
					}
				}
			} 
		}
		
		return result;
	}
	
	public static void main(String[] args) {
		DeviceMapping mapping = new DeviceMapping();
		mapping.load("conf/check_test");
		System.out.println(mapping.deviceMappings.size());
		Device result =mapping.mapping(new Device("","-TL00",null));
		System.out.println(result);
	}
	
}

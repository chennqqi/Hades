package com.aotain.project.gdtelecom.ua.mapping;

import org.apache.hadoop.conf.Configuration;

import com.aotain.project.gdtelecom.ua.pojo.Device;

/**
 * 终端解析，解析UA，获取得终端信息，如型号、品牌、名称等
 * @author Administrator
 *
 */
public class DeviceParse {

	private UAParse uaParse;
	private DeviceMapping mapping;
	
	/**
	 * 初始化配置信息
	 * @param conf
	 */
	public void init(Configuration conf){
		uaParse = new UAParse();
		uaParse.load(conf.get("CONFIG_FILE"), conf);
		mapping = new DeviceMapping();
		mapping.load(conf.get("CHECK_CONF"),conf);
	}
	
	/**
	 * 从UA取获取Device，并从check表中匹配出 名称和品牌，注：使用前必须先初始化init
	 * @param ua
	 * @return 若ua中获取不到，则device==null,
	 * 			若从ua获取到device，但check表中无法匹配出名称，则device.getName()==null
	 */
	public Device getDevice(String ua){
		// 从终端获取终端信息（类型、型号、品牌）
		Device device = uaParse.getDevice(ua);
		if(device != null && null != device.getModel() && null == device.getName()) {
			// 从check表中匹配出相应的名称
			Device mapped =  mapping.mapping(device);
			if(mapped != null) {
				device.setName(mapped.getName());
				device.setVendor(mapped.getVendor());
				device.setType(mapped.getType());
			} 
		} 
		return device;
	}
	
	public boolean containBlock(String domain, String ua){
		return uaParse.containBlock(domain, ua);
	}
	
	
	
	public UAParse getUaParse() {
		return uaParse;
	}

	public static void main(String[] args) {
	}
	
}

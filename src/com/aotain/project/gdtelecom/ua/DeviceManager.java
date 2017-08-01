package com.aotain.project.gdtelecom.ua;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aotain.project.gdtelecom.ua.pojo.Device;

/**
 * 暂无使用
 * @author Administrator
 *
 */
public class DeviceManager {

	private  Map<String, List<Device>> devices = new HashMap<String,List<Device>>();
	
	private  void load() throws Exception{
		String file ="E:\\work\\dev\\6.省电信异网项目\\终端爬取\\mr处理后数据\\device";
		BufferedReader br  = new BufferedReader(new InputStreamReader(new FileInputStream(file), "utf-8"));
		String line = null;
		
		while((line = br.readLine()) != null) {
			Device device= new Device();
			String[] arr = line.split("#");
			device.setModel(arr[0].toUpperCase());
			device.setName(arr[1]);
			device.setVendor(arr[2]);
			addDevice(device);
		}
		br.close();
	}
	
	/**
	 * 找出最匹配的Device
	 * @param ventor 品牌，如果不为空，则从该品牌下的型号下进行匹配，否则全量匹配
	 * @param model 型号
	 * @return
	 */
	private Device match(String ventor, String model) {
		if(ventor != null && !ventor.trim().equals("")) {
			List<Device> list = getVentorDevices(ventor);
			
		}
		return null;
	}
	
	private List<Device> getVentorDevices(String ventor) {
		return devices.get(ventor);
	}
	
	private  void addDevice(Device device) {
		if(devices.containsKey(device.getVendor())) {
			devices.get(device.getVendor()).add(device);
		} else {
			List<Device> list = new  ArrayList<Device>();
			list.add(device);
			devices.put(device.getVendor(), list);
		}
	}
	
	private void printDevice() {
		for(String key : devices.keySet()) {
			List<Device> list = devices.get(key);
			for(Device d : list) {
				System.out.println(d);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		DeviceManager man = new DeviceManager();
		man.load();
		man.printDevice();
	}
}

package dmpcommon.config;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.aotain.common.CommonFunction;

import dmpcommon.pojo.Device;
import dmpcommon.pojo.DeviceProperties;
import dmpcommon.pojo.DeviceRegex;
import dmpcommon.pojo.DeviceType;
import dmpcommon.pojo.PropNameEnum;

public class UAConfig {

	/**
	 * ua解析配置文件数据 ，存放各型号解析的正则
	 */
	private List<DeviceRegex> devices = new ArrayList<DeviceRegex>();
	
	/**
	 * 过滤掉的ua
	 */
	private Set<String> filter = new HashSet<String>();
	
	/**
	 * 映射配置数据，key->ua中的 型号   value->Device
	 */
	private Map<String, Device> deviceMappings = new HashMap<String, Device>();
	
	/**
	 * 从本地加载配置
	 * @param confFile 本地文件路径
	 */
	public void loadConf(String confFile) {
		InputStream in = null;
		try {
			in = new FileInputStream(confFile);
			loadConf2Cache(in);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new RuntimeException("加载配置异常", e);
		} finally{
			if(null != in) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 从本地加载配置
	 * @param checkFile check配置文件路径
	 */
	public void loadCheck(String checkFile) {
		InputStream checkin = null;
		try {
			checkin = new FileInputStream(checkFile);
			loadCheck2Cache(checkin);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new RuntimeException("加载配置异常", e);
		} finally{
			close(checkin, null);
		}
	}
	
	/**
	 * 加载mapping配置
	 * @param in
	 */
	public void loadCheck2Cache(InputStream in) {
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
			System.out.println("check数据加载完成,size:" + deviceMappings.size());
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			close(null, br);
		}
	}
	
	/**
	 * 加载配置
	 * @param in
	 */
	public void loadConf2Cache(InputStream in) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(in));
			String line = null;
			while((line=br.readLine())!=null) {
				line= line.trim();
				if(line.startsWith("#")) {
					continue;
				}
				if(line.contains("#")) {
					line = line.substring(0, line.indexOf("#"));
				}
				if(line.startsWith("filter:")) {
					filter.add(line.substring("filter:".length()).toUpperCase());
					continue;
				}
				String[] arr = line.split("@@");
				if(arr.length<2){
					continue;
				}
				DeviceRegex t = new DeviceRegex();
				t.setRegex(arr[0].trim());
				String[] pros = arr[1].split("\\|");
				for(int i=0, len=pros.length; i<len; i++) {
					String pro  = pros[i].trim().toUpperCase();
					
					DeviceProperties deviceProperties= new DeviceProperties();
					if(pro.contains("=")){
						deviceProperties.setKey(PropNameEnum.nameOf(pro.split("=")[0]));
						deviceProperties.setValue(pro.split("=")[1]);
					} else {
						deviceProperties.setKey(PropNameEnum.nameOf(pro));
					}
					t.addProp(deviceProperties);
				}
				devices.add(t);
			}
			System.out.println("正则表达式配置加载完成，size：" + devices.size());
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
	
	public List<DeviceRegex> getDevices() {
		return devices;
	}

	public void setDevices(List<DeviceRegex> devices) {
		this.devices = devices;
	}

	public Set<String> getFilter() {
		return filter;
	}

	public void setFilter(Set<String> filter) {
		this.filter = filter;
	}
	
	
	public Map<String, Device> getDeviceMappings() {
		return deviceMappings;
	}

	public void setDeviceMappings(Map<String, Device> deviceMappings) {
		this.deviceMappings = deviceMappings;
	}

	public static void main(String[] args) {
		UAConfig conf = new UAConfig();
		conf.loadConf("conf/ua_device.conf");
		System.out.println(conf.getDevices());
	}
	
}

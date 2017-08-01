package com.aotain.project.tm.parse.common.conf;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.aotain.project.tm.parse.common.pojo.Device;

public class ModelMapping {

	private List<Device> deviceList = new ArrayList<Device>();
	
	/** Map<Type, Map<Brand,List<Device>>> */
	private Map<String, Map<String,List<Device>>> typeBrandMap = new HashMap<String, Map<String,List<Device>>>();
	/** Map<Brand, List<Device>> */
	private Map<String, List<Device>> brandMap = new HashMap<String, List<Device>>();
	/** Map<Type, List<Device>> */
	private Map<String, List<Device>> typeMap = new HashMap<String, List<Device>>();
	
	
	public List<Device> getDevice(String type, String brand){
		Map<String,List<Device>> _brandMap = typeBrandMap.get(type);
		if(null == _brandMap){
			return null;
		}
		return _brandMap.get(brand);
	}
	
	public List<Device> getDeviceFromBrand(String brand){
		return brandMap.get(brand);
	}
	
	public List<Device> getDeviceFromType(String type){
		return typeMap.get(type);
	}
	
	/**
	 * 从本地加载配置
	 * @param confFile 本地文件路径
	 */
	public void load(String confFile) {
		InputStream in = null;
		try {
			in = new FileInputStream(confFile);
			load(in);
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
	 * 加载配置
	 * @param in
	 */
	private void load(InputStream in) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(in, "utf-8"));
			String line = null;
			while((line=br.readLine())!=null) {
				line= line.trim();
				String[] arr = line.split("\\|", 5);
				if(arr.length < 4){
					System.out.println("error:"+line);
					continue;
				}
				String model = arr[0].toUpperCase();
				String name = arr[1].toUpperCase();
				String brand = arr[2].toUpperCase();
				String type = arr[3].toUpperCase();
				Device d = new Device(model,name,type,brand);
				add2Cache(d);
			}
			System.out.println("加载终端品牌配置完成：size=" + deviceList.size());
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			try {
				if(br !=null) {
					br.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void add2Cache(Device d){
		deviceList.add(d);
		List<Device> brandClassList = brandMap.get(d.getBrand());
		if(brandClassList == null){
			brandClassList  = new ArrayList<Device>();
			brandMap.put(d.getBrand(), brandClassList);
		}
		brandClassList.add(d);
		
		List<Device> typeClassList = typeMap.get(d.getType());
		if(typeClassList == null){
			typeClassList  = new ArrayList<Device>();
			typeMap.put(d.getType(), typeClassList);
		}
		typeClassList.add(d);
		
		Map<String,List<Device>> typeClassMap = typeBrandMap.get(d.getType());
		if(typeClassMap == null){
			typeClassMap = new HashMap<String,List<Device>> ();
			typeBrandMap.put(d.getType(), typeClassMap);
		} 
		List<Device> typeBrandClassList = typeClassMap.get(d.getBrand());
		if(typeBrandClassList == null){
			typeBrandClassList  = new ArrayList<Device>();
			typeClassMap.put(d.getBrand(), typeBrandClassList);
		}
		typeBrandClassList.add(d);
	}
	
	/**
	 * 从HDFS加载配置
	 * @param hdfsFile hdfs文件路径
	 * @param conf
	 */
	public void load(String hdfsFile, Configuration conf) {
		InputStream in = null;
		try {
			System.out.println("加载device数据路径:hdfs_path=" + hdfsFile);
			FileSystem fs = FileSystem.get(conf);
			Path  path = new Path(hdfsFile);
			if(fs.exists(path)) {
				for(FileStatus  filestatus : fs.listStatus(path)) {
					System.out.println("加载device数据:file=" + filestatus.getPath());
					in = fs.open(filestatus.getPath());
					load(in);
					close(in, null);
				}
			}
			System.out.println("device数据加载完成,size:" + deviceList.size());
			if(deviceList.size() == 0) {
				throw new RuntimeException("device数据为空，请检查,hdfs_path="+ hdfsFile);
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("加载配置异常", e);
		}finally{
			if(null != in) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
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
	
	
	public List<Device> getDeviceList() {
		return deviceList;
	}

	public void setDeviceList(List<Device> deviceList) {
		this.deviceList = deviceList;
	}

	public Map<String, Map<String, List<Device>>> getTypeBrandMap() {
		return typeBrandMap;
	}

	public void setTypeBrandMap(Map<String, Map<String, List<Device>>> typeBrandMap) {
		this.typeBrandMap = typeBrandMap;
	}

	public Map<String, List<Device>> getBrandMap() {
		return brandMap;
	}

	public void setBrandMap(Map<String, List<Device>> brandMap) {
		this.brandMap = brandMap;
	}

	public static void main(String[] args) {
		ModelMapping m = new ModelMapping();
		m.load("config/devicemodel.txt");
		System.out.println(m.getDeviceFromBrand("海尔"));
		System.out.println(m.getDeviceFromType("BOX"));
		System.out.println(m.getDevice("MOBILE", "海尔"));
	}
}


package com.aotain.project.gdtelecom.ua.mapping;

import org.apache.hadoop.conf.Configuration;

import com.aotain.project.gdtelecom.ua.pojo.Device;

/**
 * �ն˽���������UA����ȡ���ն���Ϣ�����ͺš�Ʒ�ơ����Ƶ�
 * @author Administrator
 *
 */
public class DeviceParse {

	private UAParse uaParse;
	private DeviceMapping mapping;
	
	/**
	 * ��ʼ��������Ϣ
	 * @param conf
	 */
	public void init(Configuration conf){
		uaParse = new UAParse();
		uaParse.load(conf.get("CONFIG_FILE"), conf);
		mapping = new DeviceMapping();
		mapping.load(conf.get("CHECK_CONF"),conf);
	}
	
	/**
	 * ��UAȡ��ȡDevice������check����ƥ��� ���ƺ�Ʒ�ƣ�ע��ʹ��ǰ�����ȳ�ʼ��init
	 * @param ua
	 * @return ��ua�л�ȡ��������device==null,
	 * 			����ua��ȡ��device����check�����޷�ƥ������ƣ���device.getName()==null
	 */
	public Device getDevice(String ua){
		// ���ն˻�ȡ�ն���Ϣ�����͡��ͺš�Ʒ�ƣ�
		Device device = uaParse.getDevice(ua);
		if(device != null && null != device.getModel() && null == device.getName()) {
			// ��check����ƥ�����Ӧ������
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
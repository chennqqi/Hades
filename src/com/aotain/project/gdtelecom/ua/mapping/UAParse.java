package com.aotain.project.gdtelecom.ua.mapping;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;

import com.aotain.common.CommonFunction;
import com.aotain.project.gdtelecom.conf.ConfManager;
import com.aotain.project.gdtelecom.ua.pojo.Device;
import com.aotain.project.gdtelecom.ua.pojo.DeviceProperties;
import com.aotain.project.gdtelecom.ua.pojo.DeviceRegex;
import com.aotain.project.gdtelecom.ua.pojo.DeviceType;

/**
 * UA����
 * ����ua�����������ͺš�Ʒ��
 * ����ƻ���ֻ���ֱ�ӽ���������
 * @author Liangsj
 *
 */
public class UAParse {

	private ConfManager confManager;
	
	// ��¼ÿ������ƥ����ϵĴ���
	private Map<String, Integer> matchRegex = new HashMap<String, Integer>();
	private static final int DEVICE_MODEL_MAX_LENGTH = 30;
	
	public  UAParse() {
		confManager = new ConfManager();
		 
		/*iphoneMap.put("IPHONE7P","ƻ��IPHONE 7 PLUS��ȫ��ͨ��");
		iphoneMap.put("IPHONE7","ƻ��IPHONE 7�����ʰ�/ȫ��ͨ��");
		iphoneMap.put("IPHONE6SP","ƻ��IPHONE 6S PLUS��ȫ��ͨ��");
		iphoneMap.put("IPHONE6P","ƻ��IPHONE 6 PLUS��ȫ��ͨ��");
		iphoneMap.put("IPHONE6S","ƻ��IPHONE 6S��ȫ��ͨ��");
		iphoneMap.put("IPHONE6","ƻ��IPHONE 6��ȫ��ͨ��");
		iphoneMap.put("IPHONE5SE","ƻ��IPHONE SE��ȫ��ͨ��");
		iphoneMap.put("IPHONESE","ƻ��IPHONE SE��ȫ��ͨ��");
		iphoneMap.put("IPHONE5S","ƻ��IPHONE 5S��˫4G��");
		iphoneMap.put("IPHONE5C","ƻ��IPHONE 5C��˫3G��");
		iphoneMap.put("IPHONE5","ƻ��IPHONE 5��32GB��");
		iphoneMap.put("IPHONE4S","ƻ��IPHONE 4S��16GB��");
		iphoneMap.put("IPHONE4","ƻ��IPHONE 4��8GB��");*/
	}
	
	public void load(String hdfsFile, Configuration conf) {
		confManager.load(hdfsFile, conf);
		for(DeviceRegex device : confManager.getDevices()) {
			matchRegex.put(device.getRegex(), 0);
		}
	}
	
	public void load(String confFile) {
		confManager.load(confFile);
		for(DeviceRegex device : confManager.getDevices()) {
			matchRegex.put(device.getRegex(), 0);
		}
	}
	
	/**
	 * ��ȡ�ն���Ϣ
	 * @param ua
	 * @return ��ȡ��������null
	 */
	public Device getDevice(String ua) {
		String _ua = ua.toUpperCase();
		// ������Ϲ���������UA������������ֱ�ӷ���
//		CalTimeUtil.start("filter");
		if(filter(_ua)) {
			return null;
		}
//		CalTimeUtil.end("filter");
		// �����������ʽ��ʽ��ȡ�ն��ͺš����͡�Ʒ��
//		CalTimeUtil.start("regex");
		Device device = getApple(_ua);
		if(device !=null) {
			return device;
		}
		device = getDeviceFromRegex(_ua);
//		CalTimeUtil.end("regex");
		// ����������ʽ��ʽ�޷���ȡ
//		CalTimeUtil.start("string");
		if(null == device) {
			String model = getDeviceModelString(_ua);
			if(null != model){
				device = new Device();
				if(_ua.contains("TABLET")) {
					device.setType(DeviceType.PAD);
					model = replace(model, "[\\s-_]*tablet", " ");
					device.setModel(model);
				} else {
					device.setModel(model.trim());
					device.setType(DeviceType.UNKNOWN);
				}
			}
		}
		// �ͺ��а�����������ģ�����null
		if(null == device || CommonFunction.isContainChinese(device.getModel())) {
			return null;
		}
//		CalTimeUtil.end("string");
		return device;
	}
	
	/**
	 * �Ƿ���Ҫ���˵�
	 * @param ua
	 * @return 
	 */
	public boolean filter(String ua) {
		if(confManager.getFilter().contains(ua)) {
			return true;
		}
		return false;
	}
	
	public boolean containBlock(String domain, String ua){
		return confManager.containBlock(domain, ua);
	}
	
	public String replace(String str, String regex, String replacement) {
		return Pattern.compile(regex, Pattern.CASE_INSENSITIVE).matcher(str).replaceAll(Matcher.quoteReplacement(replacement));
	}
	
	/**
	 * ������ƻ���ֻ�
	 * @param ua
	 * @return
	 */
	public Device getApple(String ua) {
		if(ua.indexOf("IPHONE") > -1) {
			ua = ua.replaceAll("[\\s-_]", "");// ȥ�ո��-
			for(Map.Entry<String, String> kv : confManager.getIphoneMap().entrySet()) {
				if(ua.indexOf(kv.getKey()) > -1 ) {
					Device result  = new Device(kv.getValue(), kv.getKey(), "ƻ��", DeviceType.MOBILE);
					return result;
				}
			}
			if(ua.indexOf("IPHONE;") > -1 && ua.indexOf("IPAD") < 0) {
				return new Device("����", "IPHONE", "ƻ��", DeviceType.MOBILE);
			}
		}
		return null;
	}
	
	/**
	 * �������ʽ��ʽ��ȡ�ն��ͺš����͡�Ʒ��
	 * @param UA
	 * @return
	 */
	public Device getDeviceFromRegex(String UA) {
		if(null == UA || "".equals(UA.trim())){
			return null;
		}
		Device device = null;
		
		List<DeviceRegex> devices = confManager.getDevices();
		for(DeviceRegex t : devices) {
			try{
				Matcher pat = Pattern.compile(t.getRegex(), Pattern.CASE_INSENSITIVE).matcher(UA);
				if(pat.find()) {
					if(matchRegex.containsKey(t.getRegex())) {
						matchRegex.put(t.getRegex(), matchRegex.get(t.getRegex()) + 1);
					} 
					device = new Device();
					device.setRegex(t.getRegex());
					int matchSize = pat.groupCount();
					for(int i=0; i < t.getProps().size(); i++) {
						DeviceProperties prop = t.getProps().get(i);
						if(null == prop.getValue()) {
							if(i <= matchSize - 1) {
								device.setProperty(prop.getKey(), pat.group(i+1));
							}
						} else {
							device.setProperty(prop.getKey(), prop.getValue());
						}
					}
					break;
				}
			} catch (Exception e) {
				System.out.println("�������ʽ���ò��ԣ�" + t.getRegex());
				e.printStackTrace();
			}
		}
		// ��ȡ�����ͺ������ȹ���������Ϊ����
		if(null == device || null == device.getModel() || device.getModel().length() > DEVICE_MODEL_MAX_LENGTH) {
			return null;
		}
		return device;
	}
	
	/**
	 * ��ȡ�ն��ͺ�
	 * @param UA
	 * @return
	 */
	public String getDeviceModelString(String UA) {
		try {
			if (UA.indexOf("BUILD") != -1) {
				UA = UA.substring(0, UA.indexOf("BUILD"));
				if (UA.indexOf(";") != -1)
					return UA.substring(UA.lastIndexOf(";") + 1, UA.length());
			}
			return null;
		} catch (Exception ex) {
			return null;
		}
	}
	
	public  void printRegexMap() {
		Set<String> keySet = matchRegex.keySet();
		for(String key : keySet) {
			System.out.println(key + " ==== " + matchRegex.get(key));
		}
	}
	
	public ConfManager getConfManager() {
		return confManager;
	}

	public static void main(String[] args) {
		UAParse uaUtil = new UAParse();
		uaUtil.load("conf/ua_device.conf");
		Device device  = uaUtil.getDeviceFromRegex("Dalvik/1.6.0 (Linux; U; Android 4.4; Nexus 500 Build/KRT16M)");
		System.out.println(device);		
//		testFile();
	}
	
	public static void testFile() {
		UAParse uaUtil = new UAParse();
		uaUtil.load("conf/ua_device.conf");
		InputStream in = null;
		BufferedReader br = null;
		PrintWriter pw = null;
		try {
			in = new FileInputStream("E:\\work\\dev\\6.ʡ����������Ŀ\\�Ż�\\iphone.ua");
			pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream("E:\\work\\dev\\6.ʡ����������Ŀ\\�Ż�\\iphone.ua.parse"),"gbk" ));
			br = new BufferedReader(new InputStreamReader(in));
			String line = null;
			while ((line = br.readLine()) != null) {
				Device device = uaUtil.getDeviceFromRegex(line.toUpperCase());
				if (device != null) {
					System.out.println(device.getModel() + " ---->> " + line);
				}
			}
			pw.close();
		} catch (Exception e) {
			e.printStackTrace();
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
}
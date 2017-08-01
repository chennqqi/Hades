package dmpcommon.ua;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aotain.common.CommonFunction;

import dmpcommon.pojo.Device;
import dmpcommon.pojo.DeviceProperties;
import dmpcommon.pojo.DeviceRegex;
import dmpcommon.pojo.DeviceType;

/**
 * UA解析
 * 根据ua，解析出来型号、品牌
 * 部分苹果手机能直接解析出名称
 * @author Liangsj
 *
 */
public class UAParse {

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
	
	
	// 记录每个正则匹配得上的次数
	private Map<String, Integer> matchRegex = new HashMap<String, Integer>();
	private static final int DEVICE_MODEL_MAX_LENGTH = 30;
	private Map<String, String> iphoneMap;
	
	public  UAParse() {
		iphoneMap = new TreeMap<String,String>(new Comparator<String>() {
				@Override
				public int compare(String o1, String o2) {
					return o1.length() > o2.length() ? -1 : 1;
				}
			});
		 
		iphoneMap.put("IPHONE7P","苹果iPhone 7 Plus（全网通）");
		iphoneMap.put("IPHONE7","苹果iPhone 7（国际版/全网通）");
		iphoneMap.put("IPHONE6SP","苹果iPhone 6S Plus（全网通）");
		iphoneMap.put("IPHONE6P","苹果iPhone 6 Plus（全网通）");
		iphoneMap.put("IPHONE6S","苹果iPhone 6S（全网通）");
		iphoneMap.put("IPHONE6","苹果iPhone 6（全网通）");
		iphoneMap.put("IPHONE5SE","苹果iPhone SE（全网通）");
		iphoneMap.put("IPHONESE","苹果iPhone SE（全网通）");
		iphoneMap.put("IPHONE5S","苹果iPhone 5S（双4G）");
		iphoneMap.put("IPHONE5C","苹果iPhone 5C（双3G）");
		iphoneMap.put("IPHONE5","苹果iPhone 5（32GB）");
		iphoneMap.put("IPHONE4S","苹果iPhone 4S（16GB）");
		iphoneMap.put("IPHONE4","苹果iPhone 4（8GB）");
	}
	
	
	/**
	 * 从UA取获取Device，并从check表中匹配出 名称和品牌，注：使用前必须先初始化init
	 * @param ua
	 * @return 若ua中获取不到，则device==null,
	 * 			若从ua获取到device，但check表中无法匹配出名称，则device.getName()==null
	 */
	public Device getDevice(String ua){
		// 从终端获取终端信息（类型、型号、品牌）
		Device device = parseUA(ua);
		if(device != null && null != device.getModel() && null == device.getName()) {
			// 从check表中匹配出相应的名称
			Device mapped =  mapping(device);
			if(mapped != null) {
				device.setName(mapped.getName());
				device.setVendor(mapped.getVendor());
				device.setType(mapped.getType());
			} 
		} 
		return device;
	}
	
	/**
	 * 获取终端信息
	 * @param ua
	 * @return 获取不到返回null
	 */
	public Device parseUA(String ua) {
		String _ua = ua.toUpperCase();
		// 如果符合过滤条件的UA，不作处理，直接返回
//		CalTimeUtil.start("filter");
		if(filter(_ua)) {
			return null;
		}
//		CalTimeUtil.end("filter");
		// 先用正则表达式方式获取终端型号、类型、品牌
//		CalTimeUtil.start("regex");
		Device device = getApple(_ua);
		if(device !=null) {
			return device;
		}
		device = getDeviceFromRegex(_ua);
//		CalTimeUtil.end("regex");
		// 如果正则表达式方式无法获取
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
		// 型号中包含乱码或中文，返回null
		if(null == device || CommonFunction.isContainChinese(device.getModel())) {
			return null;
		}
//		CalTimeUtil.end("string");
		return device;
	}
	
	/**
	 * 是否需要过滤掉
	 * @param ua
	 * @return 
	 */
	public boolean filter(String ua) {
		if(filter.contains(ua)) {
			return true;
		}
		return false;
	}
	
	public String replace(String str, String regex, String replacement) {
		return Pattern.compile(regex, Pattern.CASE_INSENSITIVE).matcher(str).replaceAll(Matcher.quoteReplacement(replacement));
	}
	
	/**
	 * 解析出苹果手机
	 * @param ua
	 * @return
	 */
	public Device getApple(String ua) {
		if(ua.indexOf("IPHONE") > -1) {
			ua = ua.replaceAll("[\\s-_]", "");// 去空格和-
			for(Map.Entry<String, String> kv : iphoneMap.entrySet()) {
				if(ua.indexOf(kv.getKey()) > -1 ) {
					Device result  = new Device(kv.getValue(), kv.getKey(), "苹果", DeviceType.MOBILE);
					return result;
				}
			}
			if(ua.indexOf("IPHONE;") > -1 && ua.indexOf("IPAD") < 0) {
				return new Device("其他", "IPHONE", "苹果", DeviceType.MOBILE);
			}
		}
		return null;
	}
	
	/**
	 * 正则表达式方式获取终端型号、类型、品牌
	 * @param UA
	 * @return
	 */
	public Device getDeviceFromRegex(String UA) {
		if(null == UA || "".equals(UA.trim())){
			return null;
		}
		Device device = null;
		
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
				System.out.println("正则表达式配置不对：" + t.getRegex());
				e.printStackTrace();
			}
		}
		// 获取到的型号若长度过长，则认为不对
		if(null == device || null == device.getModel() || device.getModel().length() > DEVICE_MODEL_MAX_LENGTH) {
			return null;
		}
		return device;
	}
	
	/**
	 * 获取终端型号
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
	
	
}

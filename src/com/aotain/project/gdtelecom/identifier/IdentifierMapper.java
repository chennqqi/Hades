package com.aotain.project.gdtelecom.identifier;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aotain.common.CommonFunction;
import com.aotain.common.ObjectSerializer;
import com.aotain.project.gdtelecom.conf.ConfManager;
import com.aotain.project.gdtelecom.identifier.adapter.AppAdapter;
import com.aotain.project.gdtelecom.identifier.adapter.IMEIAdapter;
import com.aotain.project.gdtelecom.identifier.enums.Attcode;
import com.aotain.project.gdtelecom.ua.mapping.DeviceParse;
import com.aotain.project.gdtelecom.ua.pojo.Device;
import com.aotain.project.gdtelecom.ua.util.Constant;

public class IdentifierMapper<KEYIN, VALUEIN> extends Mapper<KEYIN, VALUEIN, Text, Text> {

	protected Map<String, String> phonemap = new HashMap<String, String>();
	protected Map<String, String> app = new HashMap<String, String>();
	protected DeviceParse deviceParse = new DeviceParse();
	protected AppAdapter appAdapter = new AppAdapter();
	protected IMEIAdapter imeiAdapter = new IMEIAdapter();
	protected ConfManager conf;
	
	private static final String OUT_SPLIT = "|";
	private static final String REGEX_USERNAME = "^([a-zA-Z0-9_\\-\\.]{1,50})@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.)|(([a-zA-Z0-9\\-]+\\.)+))(com|cn|net|com.cn|org|gd)(\\]?)$";
	private static final String REGEX_MAIL = "mail(=|:)([\\w[.-]]+@[\\w[.-]]+\\.[\\w]+)[;&,}\\s*]{1}";
	private static final String REGEX_IDFA = "(idfa|idfv)(=|:)(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}";
	private static final String REGEX_IMEI = "imei(=|:|_)(([1-9]{1})\\d{13,14})[;&_,}\\s*]{1}";
	private static final String REGEX_MAC = "(mac|macaddress)(=|:|_)([0-9a-z]{2}(:|-)[0-9a-z]{2}(:|-)[0-9a-z]{2}(:|-)[0-9a-z]{2}(:|-)[0-9a-z]{2}(:|-)[0-9a-z]{2})[;&_,}\\s*]{1}";
	private static final String REGEX_PHONE = "(=|:)([1][0-9]{10})[;&,}\\s*]{1}";
	private static final String REGEX_QQ = "([1-9]\\d{4,11})";
	private static final String REGEX_IMSI = "imsi(=|:|@|_)(([1-9]{1})\\d{14})[;&_,}\\s*]{1}";

	private static final String REGEX_PHONE_ALL="[;?&,}\\s*]{1}(\\w+)(:|=)([1][0-9]{10})[;&,}\\s*]{1}";
	
	protected Text outvalue = new Text();
	protected Text outkey = new Text();

	protected String sUserName;
	protected String url;
	protected String cookie;
	protected String pack_contnt;
	protected String domain;
	protected String rootDomain;
	protected String ua;
	protected String ip;
	protected String port;
	protected Device device;
	protected String appName;
	protected String imei;
	protected int weight_device;
	protected static final int WEIGHT_MAX  = 3;;

	protected void reset(){
		sUserName = null;
		url = null;
		cookie = null;
		pack_contnt = null;
		domain = null;
		rootDomain = null;
		ua = null;
		ip = null;
		port = null;
		device = null; 
		appName = null;
		imei = null;
	}
	
	protected void handleBase()
			throws Exception {
		if (url != null) {
			if (url.contains("%")) {
				url = url.replaceAll("%(?![0-9a-fA-F]{2})", "%25");
				url = java.net.URLDecoder.decode(url, "utf-8");
			}
			url = url.replace("\"", "").toLowerCase();
		}

		if (cookie != null) {
			if (cookie.contains("%")) {
				cookie = cookie.replaceAll("%(?![0-9a-fA-F]{2})", "%25");
				cookie = java.net.URLDecoder.decode(cookie, "utf-8");
			}
			cookie = cookie.replace("\"", "").toLowerCase();
		}

		if (domain != null) {
			String domains[] = domain.split("\\.");
			int domainLength = domains.length;
			if (domainLength > 2) {
				rootDomain = domain.substring(domain.indexOf(".") + 1);
			} else {
				rootDomain = domain;
			}
		} else {
			rootDomain = "null.com";
		}

	}

	@Override
	protected void setup(Mapper<KEYIN, VALUEIN, Text, Text>.Context context) throws IOException, InterruptedException {
		String str = context.getConfiguration().get("map");
		phonemap = (HashMap<String, String>) ObjectSerializer.deserialize(str);
		deviceParse.init(context.getConfiguration());
		appAdapter.load(context.getConfiguration().get("APP_CONF"), context.getConfiguration()); 
		imeiAdapter.load(context.getConfiguration().get("IMEI_CONF"), context.getConfiguration());
		conf = deviceParse.getUaParse().getConfManager();
		
		app.put("kepler.jd.com",
				"1::deviceid=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
		app.put("market.m.sjzhushou.com",
				"1::ifa:(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
		app.put("yktd.m.cn.miaozhen.com",
				"2::(m0|m5)=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
		app.put("vyk.admaster.com.cn",
				"2::(o|z)(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
		app.put("m.game.weibo.cn",
				"1::deviceid=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
		app.put("ad.ximalaya.com",
				"2::(adid|udid)=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
		app.put("54.222.190.235",
				"1::tdid=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
		app.put("capi.douyucdn.cn",
				"1::devid=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
		app.put("ark.letv.com",
				"1::did=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
		app.put("passport.iqiyi.com",
				"1::device_id=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
		app.put("paopao.iqiyi.com",
				"1::m_device_id=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
		app.put("search.video.qiyi.com",
				"1::u=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
		app.put("subscription.iqiyi.com",
				"1::ckuid=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
		app.put("iface.iqiyi.com",
				"2::(qyid|cupid_uid)=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
		app.put("iface2.iqiyi.com",
				"2::(qyid|cupid_uid)=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
		app.put("msg.71.am",
				"2::(u|uid)=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
		app.put("api.yuedu.iqiyi.com",
				"1::qiyiid=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
		app.put("mi.gdt.qq.com",
				"1::m5:(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");

	}

	protected static String findByRegex(String str, String regEx, int group) {
		String resultValue = null;
		if ((str == null) || (regEx == null) || ((regEx != null) && ("".equals(regEx.trim()))))
			return resultValue;

		Pattern p = Pattern.compile(regEx);
		Matcher m = p.matcher(str);

		boolean result = m.find();
		if (result) {
			resultValue = m.group(group);
		}
		return resultValue;
	}
	
	protected static String[] findAllByRegex(String str, String regEx) {
		if ((str == null) || (regEx == null) || ((regEx != null) && ("".equals(regEx.trim()))))
			return null;

		Pattern p = Pattern.compile(regEx);
		Matcher m = p.matcher(str);
		String[] results = null;
		if (m.find()) {
			results = new String[m.groupCount()];
			for(int i=0; i<results.length; i++){
					results[i] = m.group(i+1);
			}
		}
		return results;
	}

	/**
	 * 解析出APP
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	protected void app(int k, Context context) {
		/*if(device == null || device.getName() == null){
			return;
		}*/
		try {
			appName = appAdapter.getAppFromDomain(domain);
			if (appName == null) {
				appName = appAdapter.getAppFromIp(ip, port);
				if (appName == null) {
					appName = appAdapter.getAppFromUrl(url);
					if (appName == null) {
						appName = appAdapter.getAppFromUa(ua);
					}
				}
			}
			if (appName != null) {
				// APP
				contextWrite(context, Attcode.APP.getName(), appName, k);
			}
		} catch (Exception e) {
		}
	}
	
	/**
	 * 解析ua,获取终端
	 * @param k 权重
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void parseUA(int k, Context context){
		try {
			device = null;
			// 解析UA，获取终端
			if (null != ua && !ua.trim().equals("") && !CommonFunction.isMessyCode(ua)) {
				device = deviceParse.getDevice(ua);
				// terminal
				if (device != null && device.getName() != null) {
					contextWrite(context, Attcode.TERMIAL.getName(), device.getVendor() + "#" + device.getName(), k);
				}
			}
		} catch (Exception e) {
		}
		
	}

	protected void device(Context context) {
		try {
			if(imei != null) {
				device = imeiAdapter.mapping(imei);
			} 
			if(device !=null) {
				weight_device = WEIGHT_MAX;
				contextWrite(context, Attcode.TERMIAL.getName(), device.getVendor() + "#" + device.getName(), weight_device);
				contextWriteSource(context, Attcode.TERMIAL.getName(), device.getVendor() + "#" + device.getName(), ua);
			} else if (null != ua && !ua.trim().equals("") && !CommonFunction.isMessyCode(ua)) {
				if(deviceParse.containBlock(domain, ua)){
//					System.out.println("黑名单，不处理,domain-device:" + domain + "-" + ua);
					contextWrite(context, Attcode.TERMIAL_BLOCK.getName(), device.getVendor() + "#" + device.getName(), weight_device);
					contextWriteSource(context, Attcode.TERMIAL_BLOCK.getName(), device.getVendor() + "#" + device.getName(), ua);
					return;
				}
				device = deviceParse.getDevice(ua);
				
				if (device != null && device.getName() != null) {
					contextWrite(context, Attcode.TERMIAL.getName(), device.getVendor() + "#" + device.getName(), weight_device);
					contextWriteSource(context, Attcode.TERMIAL.getName(), device.getVendor() + "#" + device.getName(), ua);
				}
				
			}
			/*if (device != null && device.getName() != null) {
				contextWrite(context, IdType.TERMIAL.getId(), device.getVendor() + "#" + device.getName(), k);
			}*/
		} catch (Exception e) {
		}
	}
	
	protected void imei(int k, Context context){
		try {
			imei = findByRegex(url, REGEX_IMEI, 2);
			if (imei == null) {
				imei = findByRegex(cookie, REGEX_IMEI, 2);
				if(imei == null) {
					imei = findByRegex(pack_contnt, REGEX_IMEI, 2);
				} else {
					k++;
				}
			}
			if(imei !=null) {
				contextWrite(context, Attcode.IMEI.getName(), imei, k);
			}
		} catch (Exception e) {
		}
	}
	
	private boolean ismail(String mail){
		return mail != null && mail.length() <= 35 && !mail.endsWith(".com.cn")
				&& (mail.endsWith(".com") || mail.endsWith(".cn"));
	}
	
	protected void mail(int k, Context context) {
		try {
			String temp = findByRegex(url, REGEX_MAIL, 2);
			if (!ismail(temp)) {
				temp = findByRegex(cookie, REGEX_MAIL, 2);
				if (!ismail(temp)) {
					temp = findByRegex(pack_contnt, REGEX_MAIL, 2);
				}else {
					k++;
				}
			}
			if (temp != null) {
				contextWrite(context, Attcode.MAIL.getName(), temp, k);
			}
		} catch (Exception e) {
		} 
	}
	
	protected void mac_terminal(int k, Context context){
		try {
			if (device != null && device.getName() != null) {
				String temp = findByRegex(url, REGEX_MAC, 3);
				if (temp == null) {
					temp = findByRegex(cookie, REGEX_MAC, 3);
					if (temp == null) {
						temp = findByRegex(pack_contnt, REGEX_MAC, 3);
					}else {
						k++;
					}
				}

				if (temp != null) {
					temp = temp.replace("-", ":").toUpperCase();
					outvalue.set(k + "|" + rootDomain);
					contextWrite(context, Attcode.MAC_TERMINAL.getName(), new StringBuffer(temp).append("#")
							.append(device.getVendor()).append("#").append(device.getName()), weight_device);
					// mac_app
					if (appName != null) {
						contextWrite(context, Attcode.MAC_APP.getName(),
								new StringBuffer(temp).append("#").append(appName), k);
					}
				}
			}
		} catch (Exception e) {
		}
	}
	
	/**
	 * 解析号码
	 * 根据正则，解析出号码和号码前的关键字，号码关键在配置的关键字中，且号码前7位能在号码表中找到，则为手机号码
	 * @param k 权重 
	 * @param content 要解析的内容
	 * @param context 
	 */
	public void onephoneKey(int k, String content, Context context) {
		try {
//			String[] temp = findAllByRegex(content, REGEX_PHONE_ALL);
			if(content == null){
				return ;
			}
			Pattern p = Pattern.compile(REGEX_PHONE_ALL);
			Matcher m = p.matcher(content);
			while (m.find()) {
				String key = m.group(1);
				String phone = m.group(3);
				String p7 = phone.substring(0, 7); 
				if (phonemap.get(p7) != null) {
					String phone_attcode = conf.checkPhoneKey(key);
					if(phone_attcode !=null ) {
						contextWrite(context, phone_attcode, phone, k);
						contextWriteSource(context, phone_attcode, phone, content);
						// phone_terminal
						if (device != null && device.getName() != null) {
							contextWrite(context, Attcode.PHONE_TERMINAL.getName(), new StringBuffer(phone).append("#").append(device.getVendor()).append("#").append(device.getName()), weight_device);
						}
						// phone_app
						if(appName != null) {
							contextWrite(context, Attcode.PHONE_APP.getName(), new StringBuffer(phone).append("#").append(appName), k);
						}
					} else {
						contextWrite(context, Attcode.PHONE_NOTKEY.getName(), phone, k);
						contextWriteSource(context, Attcode.PHONE_NOTKEY.getName(), phone, content);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public void onephone(int k, String content, Context context) {
		try {
			String temp = findByRegex(content, REGEX_PHONE, 2);
			if (temp != null) {
				String p7 = temp.substring(0, 7);
				if (phonemap.get(p7) != null) {
					contextWrite(context, Attcode.PHONE.getName(), temp, k);
					contextWriteSource(context, Attcode.PHONE.getName(), temp, content);
					// phone_terminal
					if (device != null && device.getName() != null) {
						contextWrite(context, Attcode.PHONE_TERMINAL.getName(), new StringBuffer(temp).append("#").append(device.getVendor()).append("#").append(device.getName()), weight_device);
					}
					// phone_app
					if(appName != null) {
						contextWrite(context, Attcode.PHONE_APP.getName(), new StringBuffer(temp).append("#").append(appName), k);
					}
				}
			}
		} catch (Exception e) {
		}
		
	}
	
	protected void phone(int k, Context context){
		/*onephone(k, url, context);
		onephone(k + 1, cookie, context);
		onephone(k, pack_contnt, context);*/
		
		onephoneKey(k, url, context);
		onephoneKey(k + 1, cookie, context);
		onephoneKey(k, pack_contnt, context);
	}
	
	protected void imsi(int k, Context context){
		try {
			String temp = findByRegex(url, REGEX_IMSI, 2);
			if (null == temp) {
				temp = findByRegex(cookie, REGEX_IMSI, 2);
				if (null == temp) {
					temp = findByRegex(pack_contnt, REGEX_IMSI, 2);
				}
			}
			if (temp != null) {
				contextWrite(context, Attcode.IMSI.getName(), temp, k);
			}
		} catch (Exception e) {
		}
	}
	
	protected void qq(int k, Context context){
		try {
			k++;
			if (domain.contains("qq.com")) {
				if (cookie.indexOf("o_cookie") != -1) {
					String temp = cookie.substring(cookie.indexOf("o_cookie"), cookie.length());
					if (temp.indexOf(";") != -1) {
						temp = temp.substring(0, temp.indexOf(";"));
						if (temp.split("=").length == 2) {
							temp = temp.split("=")[1];
							temp = findByRegex(temp, REGEX_QQ, 0);
							if (temp != null) {
								contextWrite(context, Attcode.QQACCOUNT.getName(), temp, k);
							}
						}
					}
				}
			}
		} catch (Exception e) {
		}
	}
	
	protected void idfa(int k, Context context){
		try {
			String temp = findByRegex(url, REGEX_IDFA, 3);
			if (null == temp) {
				temp = findByRegex(cookie, REGEX_IDFA, 3);
				if (null == temp) {
					temp = findByRegex(pack_contnt, REGEX_IDFA, 3);
				}
			}
			if (temp != null) {
				contextWrite(context, Attcode.IDFA.getName(), temp, k);
			} else {
				String lishj = app.get(domain);
				if (lishj != null) {
					String[] lishjs = lishj.split("::");
					temp = findByRegex(url, lishjs[1], Integer.parseInt(lishjs[0]));
					if (temp != null) {
						temp = temp.toUpperCase();
						contextWrite(context, Attcode.IDFA.getName(), temp, k);
					}
				}
			}
		} catch (Exception e) {
		}
	}
	
	protected void contextWrite(Context context, String attcode,String attvalue, int k) throws IOException, InterruptedException{
		outvalue.set(k + "|" + rootDomain);
		StringBuffer sb = new StringBuffer();
		sb.append(attcode).append(OUT_SPLIT).append(sUserName).append(OUT_SPLIT).append(attvalue);
		outkey.set(sb.toString());
		context.write(outkey, outvalue);
	}
	
	protected void contextWriteSource(Context context, String attcode,String attvalue, String src) throws IOException, InterruptedException{
		outvalue.set("");
		StringBuffer sb = new StringBuffer();
		sb.append(Constant.NAME_OUTPUT_SOURCE).append(sUserName).append(OUT_SPLIT).append(attcode).append(OUT_SPLIT).append(attvalue).append(OUT_SPLIT).append(domain).append(OUT_SPLIT).append(src);
		outkey.set(sb.toString());
		context.write(outkey, outvalue);
	}
	
	protected void contextWrite(Context context, String id,StringBuffer attvalue,int k) throws IOException, InterruptedException{
		outvalue.set(k + "|" + rootDomain);
		StringBuffer sb = new StringBuffer();
		sb.append(id).append(OUT_SPLIT).append(sUserName).append(OUT_SPLIT).append(attvalue);
		outkey.set(sb.toString());
		context.write(outkey, outvalue);
	}

	protected static boolean validateUser(String username) {
		return CommonFunction.findByRegex(username, REGEX_USERNAME, 0) == null;
	}

	protected byte[] decode(String str) {
		byte[] bt = null;
		try {
			sun.misc.BASE64Decoder decoder = new sun.misc.BASE64Decoder();
			bt = decoder.decodeBuffer(str);
		} catch (IOException e) {
		}
		return bt;
	}

	protected String getCol(List<Object> list, int index) {
		Object obj = list.get(index);
		return null == obj ? null : obj.toString().trim();
	}

	public ConfManager getConf() {
		return conf;
	}
	
	
}

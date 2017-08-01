package dmpcommon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aotain.common.CommonFunction;
import com.aotain.common.ObjectSerializer;

import dmpcommon.config.KvItem;
import dmpcommon.pojo.Device;
import dmpcommon.pojo.DeviceRegex;
import dmpcommon.ua.UAParse;

public class KVMapper<KEYIN, VALUEIN> extends Mapper<KEYIN, VALUEIN, Text, Text> {

	protected Map<String, Integer> mapField = new HashMap<String, Integer>();
	protected Map<Integer,String> mapFilter = new HashMap<Integer,String>();
	protected Map<Integer,String> mapDecode = new HashMap<Integer,String>();
	protected Map<String, ArrayList<KvItem>> mapHost = new HashMap<String, ArrayList<KvItem>>();
	protected Map<String, String> mapPhone = new HashMap<String, String>();
	protected Map<String, String> getID = new HashMap<String, String>();
	protected String fieldsplit = "";
	protected int dtfield = -1;
	protected int dtformat = -1;
	protected String vret;
	protected boolean parseua = false;
	int hour = -1;
	int min = -1;
	String hm = null;

	protected Text outvalue = new Text();
	protected Text outkey = new Text();

	protected UAParse uaParse = new UAParse();

	@Override
	protected void setup(Mapper<KEYIN, VALUEIN, Text, Text>.Context context) throws IOException, InterruptedException {
		mapField = (Map<String, Integer>) ObjectSerializer.deserialize(context.getConfiguration().get("KvField"));
		mapFilter = (Map<Integer, String>) ObjectSerializer.deserialize(context.getConfiguration().get("KvFilter"));
		mapDecode = (Map<Integer, String>) ObjectSerializer.deserialize(context.getConfiguration().get("KvDecode"));
		mapHost = (Map<String, ArrayList<KvItem>>) ObjectSerializer
				.deserialize(context.getConfiguration().get("KvMap"));
		mapPhone = (Map<String, String>) ObjectSerializer.deserialize(context.getConfiguration().get("PhMap"));
		fieldsplit = context.getConfiguration().get("fieldsplit");
		dtfield = Integer.parseInt(context.getConfiguration().get("dtfield"));
		dtformat = Integer.parseInt(context.getConfiguration().get("dtformat"));

		List<DeviceRegex> devices = (List<DeviceRegex>) ObjectSerializer
				.deserialize(context.getConfiguration().get("UAConfMap"));
		Set<String> filter = (Set<String>) ObjectSerializer.deserialize(context.getConfiguration().get("UAConfFilter"));
		Map<String, Device> deviceMappings = (Map<String, Device>) ObjectSerializer
				.deserialize(context.getConfiguration().get("CheckMap"));
		uaParse.setDevices(devices);
		uaParse.setFilter(filter);
		uaParse.setDeviceMappings(deviceMappings);
	}

	protected void handle(Context context) {
		String[] items;
		String[] units;
		KvItem kv = null;
		ArrayList<KvItem> kvList;
		int flag = -1;
		int index = 0;

		String target = "";
		String host = "";
		String _host = "";
		String username = "";
		String ua = "";
		String url = "";
		String temp = "";

		int weight = 1;
		String p7 = "";
		Device device;

		hm = null;
		parseua = false;
		String devicestring = null;
		getID.clear();

		try {
			items = vret.split(fieldsplit, -1);
			
			if (items.length > 1 && mapField.get("USERNAME") != null && mapField.get("USERNAME") < items.length && dtfield < items.length) {
				
				for (Map.Entry<Integer, String> entry : mapFilter.entrySet())// ���l������
				{
					if (entry.getKey() < items.length) 
					{
						if(findByRegex(items[entry.getKey()], entry.getValue(), 0) == null)
							return;
					}
				}
				
				if(mapField.get("HOST") == null)
				{
					if(mapField.get("URL") != null)
					{
						index = mapField.get("URL");
						if(index>=items.length)
							return;
						url = items[index];
						if(mapDecode.get(index) != null)
						{
							url = code(url, mapDecode.get(index));
						}
						url = url.replaceFirst("^(http|https)://", "");
						int i = url.indexOf("/");
						if(i >= 0) {
							_host = host = url.substring(0, i);
						} else {
							_host = host = url;
						}
					}
				}
				else
				{
					index = mapField.get("HOST");
					if(index>=items.length)
						return;
					_host = host = items[index];
					if(mapDecode.get(index) != null)
					{
						_host = host = code(host, mapDecode.get(index));
					}
				}
				
				username = items[mapField.get("USERNAME")];

				if (_host.length() > 0) {
					units = _host.split("\\.", -1);
					for (int i = 0; i < units.length; i++) {
						if (mapHost.get(_host) == null)// ƥ��host
						{
							_host = _host.replace(units[i] + ".", "");
							continue;
						}

						kvList = mapHost.get(_host);
						_host = _host.replace(units[i] + ".", "");
						for (int k = 0; k < kvList.size(); k++) {
							kv = kvList.get(k);
							if (kv.CondMap.entrySet().size() > 0) {
								for (Map.Entry<Integer, String> entry : kv.CondMap.entrySet())// ���l��ƥ��
								{
									if (entry.getKey() < items.length){
										temp = items[entry.getKey()];
										if(mapDecode.get(entry.getKey()) != null)
										{
											temp = code(temp, mapDecode.get(entry.getKey()));
										}
										if(!temp.contains(entry.getValue()))
										{
											flag = -1;
											break;
										}
									} else
										flag = 1;
								}
							} else
								flag = 1;

							if (flag == 1)// ���Зl�����M��
							{
								if (kv.GetIndex == -1)// ֱ���xֵ������Ҫ��ȡ��
									vret = kv.GetRegx;
								else {
									if (kv.GetField >= 0 && kv.GetField < items.length) {
										target = items[kv.GetField];
										if (kv.Code != null && kv.Code.length() > 0) {
											target = code(target, kv.Code);
										}

										vret = findByRegex(target.toLowerCase(), kv.GetRegx, kv.GetIndex);
									}
								}

								if (vret != null && vret.length() > 0) {
									if (kv.ID.contains("PHONE")) {
										p7 = vret.substring(0, 7);
										if (mapPhone.get(p7) == null)
											continue;
									}
									if (kv.ID.contains("IDFA") && "00000000-0000-0000-0000-000000000000".equals(vret)) {
										continue;
									}
									
									if (kv.ID.contains("MAC")) {
										vret = vret.replace("-", ":");
									}
									
									if (kv.ID.contains("_T")) {
										if (!parseua) {
											parseua = true;
											index = mapField.get("UA");
											if(index>=items.length)
												continue;
											ua = items[index];
											if(mapDecode.get(index) != null)
												ua = code(ua, mapDecode.get(index));
											// ����UA����ȡ�ն�
											if (null != ua && ua.length() > 0 && !CommonFunction.isMessyCode(ua)) {
												device = uaParse.getDevice(ua);
												// terminal
												if (device != null && device.getName() != null) {
													devicestring = device.getVendor() + "#" + device.getName();
													vret = vret + "#" + devicestring;
												} else {
													continue;
												}
											} else {
												continue;
											}
										} else{
											if(devicestring == null)
												continue;
											vret = vret + "#" + devicestring;
										}
									}

									if(getHM(items[dtfield])){
										outkey.set(username + "|" + kv.ID + "|" + vret);
										weight = kv.Weight;
										if ((hour >= 20 && hour <= 23) || (hour >= 0 && hour <= 7))// ���r�g�ә���
										{
											weight = weight * 2;
										}
										outvalue.set(weight + "|" + host + "|" + hm);
										getID.put(kv.ID, kv.ID);
										context.write(outkey, outvalue);
									}
								}
							}
						}
					}
				}

				// ����]��ƥ�䵽host���t��ͨ�ë@ȡ��ʽ
				kvList = mapHost.get("null");
				for (int k = 0; k < kvList.size(); k++) {
					kv = kvList.get(k);
					if (getID.get(kv.ID) != null)
						continue;
					if (kv.ID.equals("T")) {
						if (!parseua) {
							parseua = true;
							index = mapField.get("UA");
							if(index >= items.length)
								continue;
							ua = items[index];
							if(mapDecode.get(index) != null)
								ua = code(ua, mapDecode.get(index));
							// ����UA����ȡ�ն�
							if (null != ua && ua.length() > 0 && !CommonFunction.isMessyCode(ua)) {
								device = uaParse.getDevice(ua);
								// terminal
								if (device != null && device.getName() != null) {
									devicestring = device.getVendor() + "#" + device.getName();
									vret = devicestring;
								} else {
									continue;
								}
							} else {
								continue;
							}
						} else{
							if(devicestring == null)
								continue;
							vret = devicestring;
						}

						if(getHM(items[dtfield])){
							outkey.set(username + "|" + kv.ID + "|" + vret);
							weight = kv.Weight;
							if ((hour >= 20 && hour <= 23) || (hour >= 0 && hour <= 7))// ���r�g�ә���
							{
								weight = weight * 2;
							}
							outvalue.set(weight + "|" + host + "|" + hm);
							getID.put(kv.ID, kv.ID);
							context.write(outkey, outvalue);
						}
					} else {
						if (kv.GetField >= 0 && kv.GetField < items.length) {
							target = items[kv.GetField];

							if (kv.Code != null && kv.Code.length() > 0) {
								target = code(target, kv.Code);
							}
							vret = findByRegex(target.toLowerCase(), kv.GetRegx, kv.GetIndex);
						}

						if (vret != null && vret.length() > 0) {
							if (kv.ID.contains("PHONE")) {
								p7 = vret.substring(0, 7);
								if (mapPhone.get(p7) == null)
									continue;
							}

							if (kv.ID.contains("IDFA") && "00000000-0000-0000-0000-000000000000".equals(vret)) {
								continue;
							}
							
							if (kv.ID.contains("MAC")) {
								vret = vret.replace("-", ":");
							}

							if (kv.ID.contains("_T")) {
								if (!parseua) {
									parseua = true;
									index = mapField.get("UA");
									if(index >= items.length)
										continue;
									ua = items[index];
									if(mapDecode.get(index) != null)
										ua = code(ua, mapDecode.get(index));
									// ����UA����ȡ�ն�
									if (null != ua && ua.length() > 0 && !CommonFunction.isMessyCode(ua)) {
										device = uaParse.getDevice(ua);
										// terminal
										if (device != null && device.getName() != null) {
											devicestring = device.getVendor() + "#" + device.getName();
											vret = vret + "#" + devicestring;
										} else {
											continue;
										}
									} else {
										continue;
									}
								} else{
									if(devicestring == null)
										continue;
									vret = vret + "#" + devicestring;
								}
							}

							if(getHM(items[dtfield])){
								outkey.set(username + "|" + kv.ID + "|" + vret);
								weight = kv.Weight;
								if ((hour >= 20 && hour <= 23) || (hour >= 0 && hour <= 7))// ���r�g�ә���
								{
									weight = weight * 2;
								}
								outvalue.set(weight + "|" + host + "|" + hm);
								getID.put(kv.ID, kv.ID);
								context.write(outkey, outvalue);
							}
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			;
		}
	}

	private String findByRegex(String str, String regEx, int group) {
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

	private String decode64(String str) {
		byte[] bt = null;
		try {
			sun.misc.BASE64Decoder decoder = new sun.misc.BASE64Decoder();
			bt = decoder.decodeBuffer(str);
			if (bt != null) {
				return new String(bt, "UTF-8");
			}
		} catch (IOException e) {
			;
		}
		return str;
	}

	private String urlDecode(String str) {
		try {
			str = str.replaceAll("%(?![0-9a-fA-F]{2})", "%25");
			str = java.net.URLDecoder.decode(str, "utf-8");
			if (str.startsWith("%")) {
				str = java.net.URLDecoder.decode(str, "utf-8");
			}
		} catch (Exception e) {
			;
		}
		return str;
	}

	private String code(String str, String codeType) {
		switch (codeType) {
		case "urldecode":
			str = urlDecode(str);
			break;
		case "decode64":
			str = decode64(str);
			break;
		case "alldecode":
			str = urlDecode(decode64(str));
		}
		return str;

	}

	private boolean getHM(String time) {
		if (hm != null) {
			return true;
		}
		if (dtformat == -1)// �r�g̎��
		{
			hour = Integer.parseInt(time.substring(8, 10));
			min = Integer.parseInt(time.substring(10, 12));
		} else {
			Date datatime = new Date(Long.parseLong(time.trim()) * 1000L);
			hour = datatime.getHours();
			min = datatime.getMinutes();
		}
		if(hour>=0 && hour<24 && min>=0 && min<60)
		{
			hm = hour + "_" + min;
			return true;
		}
		return false;
	}

}
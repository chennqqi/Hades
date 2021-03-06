package com.aotain.project.tm.parse.deviceinfoparse;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.aotain.project.tm.parse.common.conf.BrandConf;
import com.aotain.project.tm.parse.common.core.ModelHandle;
import com.aotain.project.tm.parse.common.pojo.Brand;
import com.aotain.project.tm.parse.common.pojo.DeviceType;
import com.aotain.project.tm.parse.common.utils.StringUtil;

/**
 * dim_deviceinfo解析，供匹配程序使用
 * 从终端信息，即爬虫数据解析中，解析出该终端的所有型号
 * 如P7-L09|华为Ascend P7（P7-L09/电信4G）
 * 解析出 HUAWEIP7L09、P7-L09、ASCENDP7、HUAWEIP7-L09、HUAWEIP7、P7L09、HUAWEIASCENDP7
 * 
 * @author Liangsj
 *
 */
public class DeviceParser {
	private String alias;// 别名
	private String name;
	private String brand;
	private String type;
	private String price;
	private String deviceatt;
	private BrandConf brandConf;
	/** 是否解析成功*/
	private boolean parseSuccess = false;
	private static final String SPLIT = "\\|";
	
	private static final String TIME_REGES =  ".*上市日期=((200)|(19)).*";// 上市时间2010前
	
	public DeviceParser(BrandConf brandConf){
		this.brandConf = brandConf;
	}
	
	public void parse(String line){
		String[] arr = line.split(SPLIT);
		if(arr.length < 6){
			parseSuccess = false;
			return;
		}
		setAlias(arr[0]);
		setName(arr[1]);
		setBrand(arr[2]);
		setType(arr[3]);
		setPrice(arr[4]);
		setDeviceatt(arr[5]);
		parseSuccess = true;
	}
	
	/**
	 * 获取所有型号
	 * @return
	 */
	public Set<String> getModels(){
		if(!parseSuccess){
			return null;
		}
		Set<String> models = getModelsFromName();
		models.addAll(getModelsFromAlias());
		return filter(models);
	}
	
	/**
	 * 过滤无效的型号，增加去特殊字符型号
	 * @param models
	 * @return
	 */
	public Set<String> filter(Set<String> models){
		Set<String> result = new HashSet<String>();
		Set<String> sepSet = new HashSet<String>();
		for(String m : models){
			sepSet.add(StringUtil.rmSpecialChar(m));// 去特殊字符
		}
		models.addAll(sepSet);
		for(String m : models){
			if(m.equals("MOBILE") || m.equals("BOX") || m.equals("TV") || m.equals("PAD")){
				System.out.println("无效:" + m);
				continue;
			}
			
			// 型号等于品牌的，无效
			List<Brand> k = brandConf.getFromAtts(Brand.EKEY, m, brand);
			List<Brand> n = brandConf.getFromAtts(Brand.ENAME, m, null);
			if((k != null && k.size() > 0)|| (n != null && n.size() > 0)){
				System.out.println("无效型号:" + m);
				continue;
			}
			result.add(m);
		}
		return result;
	}
	
	/**
	 * 名称拆分
	 *  三星GALAXY S7（G9300/全网通）
	 * 	a.去括号及内容，
			aa 替换中文品牌为英文，去空格
			bb 去中文，取剩下部分
		b.取（xxx/  中xxx部分为型号，但不包含GB/4G/3G/2G/RAM
	 * @return
	 */
	private Set<String> getModelsFromName(){
		Set<String> result = new HashSet<String>();
		if(!parseSuccess) {
			return result;
		}
		ModelHandle m = new ModelHandle(brandConf, brand,  name);
		m = m.removeSpace();
		
		Set<String> models = getModelFromString(m);
		result.addAll(models);
		
		// 括号里的型号
		ModelHandle bracketModel = m.bracketModel();
		if(!StringUtil.isNull(bracketModel.getValue())){
			result.add(bracketModel.getValue());
			Brand b = brandConf.getFromOneAtt(Brand.CNAME, bracketModel.getBrand(), null);
			if(b !=null){
				result.add(b.getEname() + bracketModel.getValue());// 品牌+型号
			}
		}
		
		return result;
	}
	
	/**
	 * 从别名中获取型号
	 * A8000,三星A8,盖乐世A8
	 * @return
	 */
	private Set<String> getModelsFromAlias(){
		Set<String> result = new HashSet<String>();
		if(StringUtil.isNull(alias)){
			return result;
		}
		String[] as = alias.split("[,，]");
		for(String a : as){
			ModelHandle m = new ModelHandle(brandConf, brand, a);
			m = m.removeSpace();
			result.addAll(getModelFromString(m));
		}
		return result;
	}
	
	
	/**
	 * 处理一个型号
	 * 1、替换中文品牌为英文
	 * 2、替换中文品牌key为英文，
	 * 3、12后去品牌key，
	 * 4、12后去品牌，
	 * 5、去key去品牌
	 * 6、去品牌、替换key为品牌
	 * @param str
	 * @return
	 */
	private  Set<String> getModelFromString(ModelHandle m){
		Set<String> result = new HashSet<String>();
		m = m.removeBracket();
		ModelHandle replaceChinese = m.replaceChinese();
		ModelHandle removeKey = replaceChinese.removeBrandKey();
		ModelHandle removeBrand = replaceChinese.removeBrand();
		ModelHandle removeKeyBrand = removeKey.removeBrand();
		ModelHandle replaceKeyToBrand = removeBrand.replaceKeyToBrand();
		
		if(!StringUtil.isNull(removeKey.getValue())){
			result.add(removeKey.getValue());
		}
		if(!StringUtil.isNull(replaceChinese.getValue())){
			result.add(replaceChinese.getValue());
		}
		if(!StringUtil.isNull(removeBrand.getValue())){
			result.add(removeBrand.getValue());
		}
		if(!StringUtil.isNull(removeKeyBrand.getValue())){
			result.add(removeKeyBrand.getValue());
		}
		if(!StringUtil.isNull(replaceKeyToBrand.getValue())){
			result.add(replaceKeyToBrand.getValue());
		}
		return result;
	} 
	
	/**
	 * 价格是否合法
	 * @return 数字，或x万，返回ture ,否则返回false(如未上市、概念机)
	 */
	public boolean isPriceLegal(){
		if(!parseSuccess || price.equals("")){
			return false;
		}
		price = price.replaceAll("万", "");
		try {
			Double.parseDouble(price);
		} catch (Exception e) {
			return false;
		}
		return true;
	}
	

	
	/**
	 * 上市时间是否2010年前的手机
	 * @return 记录无法解析返回true, 或类型为手机，且上市时间在2010年前，返回true,否则返回false
	 */
	public boolean isPhoneBefore2010() {
		if (!parseSuccess || (type.equals(DeviceType.MOBILE.getName()) && StringUtil.matcher(TIME_REGES, deviceatt))) {
			return true;
		}
		return false;
	}
	
	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias.trim().toUpperCase();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name.trim().toUpperCase();
	}

	public String getBrand() {
		return brand;
	}

	public void setBrand(String brand) {
		this.brand = brand.trim();
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type.trim();
	}

	public String getPrice() {
		return price;
	}

	public void setPrice(String price) {
		this.price = price.trim();
	}

	public String getDeviceatt() {
		return deviceatt;
	}

	public void setDeviceatt(String deviceatt) {
		this.deviceatt = deviceatt.trim();
	}

	public static void parseTest() throws Exception{
		BrandConf conf = new BrandConf();
		conf.load("config/brand_ch.csv");
		DeviceParser parser = new DeviceParser(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("E:\\work\\dev\\6.省电信异网项目\\终端爬虫\\device\\file\\all_20170710.txt"), "UTF-8"));
		PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream("E:\\work\\dev\\9.终端解析\\deviceInfoAnalysis\\data\\devicemodel.txt"), "UTF-8"));
		String line = null;
		while((line = br.readLine()) !=null){
			parser.parse(line);
			if(parser.parseSuccess && parser.isPriceLegal() && !parser.isPhoneBefore2010()){
				Set<String> models = parser.getModels();
				for(String m : models){
					String out = m + "|" + parser.getName() + "|" + parser.getBrand() + "|" + parser.getType() + "|";
					pw.println(out);
				}
				// SAMSUNGS7568|三星S7568（移动3G）|三星|MOBILE|
			}
			pw.flush();
		}
		br.close();
		pw.close();
		System.out.println("解析结束");
	}
	
	public static void main(String[] args) throws Exception {
		BrandConf conf = new BrandConf();
		conf.load("config/brand_ch.csv");
//		conf.printAll();
		DeviceParser parser = new DeviceParser(conf);
		String line1 ="红米 NOTE，小米note，Note 1S|小米红米Note（增强版/移动3G/2GB RAM）|小米|MOBILE|2199|上市日期=2015年03月$手机类型=4G手机，3G手机，智能手机，拍照手机，平板手机，快充手机$触摸屏类型=电容屏，多点触控$主屏尺寸=5.1英寸$主屏材质=Super AMOLED$主屏分辨率=2560x1440像素$屏幕像素密度=576ppi$屏幕技术=双面第四代康宁大猩猩玻璃$窄边框=3.5mm$屏幕占比=70.93%$4G网络=移动TD-LTE，联通TD-LTE，联通FDD-LTE，电信TD-LTE，电信FDD-LTE$3G网络=移动3G（TD-SCDMA），电信3G（CDMA2000），联通3G（WCDMA），联通2G/移动2G（GSM）$支持频段=2G：GSM B2/3/5/8 2G：CDMA 800 3G：CDMA EVDO 800 3G：WCDMA B1/2/5/8 3G：TD-SCDMA B34/39 4G：TD-LTE B38/39/40/41 4G：FDD-LTE B1/3/4/7/8/28$SIM卡=双卡，Nano SIM卡$WLAN功能=双频WIFI，IEEE 802.11 a/b/g/n/ac$导航=GPS导航，A-GPS技术，GLONASS导航，北斗导航$连接与共享=NFC，无线充电，红外遥控，WLAN热点，蓝牙4.1，OTG$操作系统=Android 5.0$核心数=真八核$CPU型号=三星 Exynos 7420$CPU频率=2.1GHz（大四核），1.5GHz（小四核）$GPU型号=Mali-T760$RAM容量=3GB$ROM容量=32GB$存储卡=不支持容量扩展$电池类型=不可拆卸式电池$电池容量=2550mAh$理论待机时间=约251小时（2G+4G）$其他硬件参数=支持快速充电$摄像头类型=双摄像头（前后）$后置摄像头=1600万像素$前置摄像头=500万像素$闪光灯=LED补光灯$光圈=f/1.9$视频拍摄=4K（3840x2160，30帧/秒）视频录制 1080p（1920×1080，30帧/秒）视频录制 720p（1280×720，30帧/秒）视频录制$拍照功能=延时自拍，连拍，自动对焦，OIS光学防抖，HDR，定时拍照，滤镜，全景拍照，慢动作，快动作，虚拟拍摄$造型设计=直板$机身颜色=星钻黑色，雪晶白色，冰玉蓝色，铂光金色$手机尺寸=143.4x70.5x6.8mm$手机重量=140g$机身材质=玻璃机身$操作类型=物理按键$感应器类型=重力感应器，光线传感器，距离传感器，指纹识别，加速传感器，心率传感器$指纹识别设计=前置指纹识别$机身接口=3.5mm耳机接口，Micro USB v2.0数据接口$其他外观参数=可扩展多项功能的后壳$音频支持=支持MP3/WAV/eAAC+/AC3/FLAC等格式$视频支持=支持MP4/DivX/XviD/WMV/H.264/H.263等格式$图片支持=支持JPEG/PNG/GIF/BMP等格式$常用功能=计算器，备忘录，日程表，电子书，闹钟，日历，录音机，情景模式，主题模式，地图软件$商务功能=飞行模式，数据备份，数据加密$包装清单=主机&nbsp;x1 数据线&nbsp;x1 耳机&nbsp;x1 充电器&nbsp;x1 说明书&nbsp;x1$保修政策=全国联保，享受三包服务$质保时间=1年$质保备注=主机1年，充电器1年，有线耳机3个月$客服电话=400-810-5858$电话备注=周一至周五：8:00-20:00；周六至周日：8:00-17:00（在线服务）$详细内容=自购机日起（以购机发票为准），如因质量问题或故障，凭厂商维修中心或特约维修点的质量检测证明，享受7日内退货，15日内换货，15日以上在质保期内享受免费保修等三包服务！注：单独购买手机配件产品的用户，请完好保存配件外包装以及发票原件，如无法提供上述凭证的，将无法进行正常的配件保修或更换。进入官网&gt;&gt;";
		String line2 ="P7-L09|华为Ascend P7（P7-L09/电信4G）|华为|MOBILE|1250|上市日期=2015年07月$手机类型=4G手机，3G手机，智能手机，拍照手机，平板手机$触摸屏类型=电容屏，多点触控$主屏尺寸=5.7英寸$主屏材质=Super AMOLED$主屏分辨率=1920x1080像素$屏幕像素密度=386ppi$窄边框=2.86mm$屏幕占比=74.05%$4G网络=移动TD-LTE，联通TD-LTE，联通FDD-LTE，电信TD-LTE，电信FDD-LTE$3G网络=移动3G（TD-SCDMA），联通3G（WCDMA），电信3G（CDMA2000），联通2G/移动2G（GSM）$支持频段=2G：CDMA 800 2G：GSM 850/900/1800/1900 3G：CDMA EVDO 800 3G：WCDMA 850/900/1900/2100 3G：TD-SCDMA B34/39 4G：TD-LTE 1900/2300/2600/2555-2575/2575-2635/2635-2655 4G：FDD-LTE 1750-1765/1765-1780/2100$SIM卡=双卡，Nano SIM卡$WLAN功能=双频WIFI，IEEE 802.11 a/b/g/n$导航=GPS导航，A-GPS技术，GLONASS导航，北斗导航$连接与共享=WALN热点，蓝牙4.1，NFC$操作系统=Android 5.1$用户界面=Touch Wiz2015$核心数=真八核$CPU型号=高通 骁龙615（MSM8939）$CPU频率=1.5GHz（大四核），1.0GHz（小四核）$GPU型号=高通 Adreno405$RAM容量=2GB$ROM容量=16GB/32GB$存储卡=MicroSD卡$扩展容量=128GB$电池类型=不可拆卸式电池$电池容量=3050mAh$摄像头类型=双摄像头（前后）$后置摄像头=1600万像素$前置摄像头=500万像素$闪光灯=LED补光灯$光圈=f/1.9$视频拍摄=1080p（1920×1080，30帧/秒）视频录制$拍照功能=感光度、白平衡、曝光，自动对焦，数码变焦广角自拍，美颜$造型设计=直板$机身颜色=魔幻金，雪域白，精灵黑$手机尺寸=158x76.8x5.9mm$手机重量=151g$机身材质=金属机身$操作类型=物理按键$感应器类型=重力感应器，光线传感器，距离传感器，指纹识别$指纹识别设计=前置指纹识别$机身接口=3.5mm耳机接口，Micro USB v2.0数据接口$音频支持=支持MIDI/MP3/AAC等格式$视频支持=支持MP4/3GP/AVC/AVI/MPEG-4等格式$图片支持=支持JPEG/PNG/GIF/BMP等格式$常用功能=秒表，计算器，电子词典，备忘录，日程表，记事本$商务功能=骚扰拦截，病毒查杀，权限管理，流量监控$保修政策=全国联保，享受三包服务$质保时间=1年$质保备注=主机1年，充电器1年，有线耳机3个月$客服电话=400-810-5858$电话备注=周一至周五：8:00-20:00；周六至周日：8:00-17:00（在线服务）$详细内容=自购机日起（以购机发票为准），如因质量问题或故障，凭厂商维修中心或特约维修点的质量检测证明，享受7日内退货，15日内换货，15日以上在质保期内享受免费保修等三包服务！注：单独购买手机配件产品的用户，请完好保存配件外包装以及发票原件，如无法提供上述凭证的，将无法进行正常的配件保修或更换。进入官网&gt;&gt;";
		String line3 = "电信版夺目3D|HTC X515d 夺目3D（电信版）|HTC|MOBILE|华为|MOBILE|2820|上市日期=2016年04月$手";
		String line4 = "CUBE U23GT 寒冰(16G), 酷比魔方 U23GT 寒冰(16G)|酷比魔方U23GT寒冰 16G|酷比魔方|PAD|2万|上市日期=2015年12月$手机类型=4G手机，3G手机，智能手机，音乐手机，拍照手机，快充手机$触摸屏类型=电容屏，多点触控$主屏尺寸=5.2英寸$主屏材质=Super AMOLED$主屏分辨率=1920x1080像素$屏幕像素密度=424ppi$窄边框=4.5mm$屏幕占比=68.33%$其他屏幕参数=2.5D弧面屏$4G网络=移动TD-LTE，联通TD-LTE，联通FDD-LTE$3G网络=移动3G（TD-SCDMA），联通3G（WCDMA），联通2G/移动2G（GSM）$支持频段=2G：GSM 850/900/1800/1900 3G：WCDMA 850/900/1900/2100 3G：TD-SCDMA 1880/2010 4G：TD-LTE B38/39/40/41 4G：FDD-LTE B1/3$SIM卡=双卡，Micro SIM卡/Nano SIM卡$WLAN功能=单频WIFI，IEEE 802.11 b/g/n$导航=GPS导航$连接与共享=WLAN热点，蓝牙4.0，OTG$操作系统=Android 5.1$用户界面=Funtouch OS 2.5$核心数=真八核$CPU频率=1.7GHz$GPU型号=Mali-T760$RAM容量=4GB$ROM容量=32GB$存储卡=MicroSD卡$扩展容量=128GB$电池类型=不可拆卸式电池$电池容量=2400mAh$其他硬件参数=双引擎闪充$摄像头类型=双摄像头（前后）$后置摄像头=1300万像素$前置摄像头=800万像素$传感器类型=CMOS$闪光灯=LED补光灯$光圈=f/2.2$摄像头特色=六镜式镜头$视频拍摄=1080p（1920×1080，30帧/秒）视频录制$拍照功能=PDAF相位对焦，急速追焦，防拖影，指纹拍照，自动对焦，慢镜头，快镜头，HDR，全景模式，夜景模式，超清画质，文档矫正，运动追踪，儿童模式，专业拍照，专业录像，趣味模式，美妆，性别识别$造型设计=直板$机身颜色=金色，银色，玫瑰金$手机尺寸=147.9x73.75x6.56mm$手机重量=135.5g$机身材质=金属机身$操作类型=触控按键$感应器类型=重力感应器，光线传感器，距离传感器，指纹识别，陀螺仪$指纹识别设计=后置指纹识别$机身接口=3.5mm耳机接口，Micro USB v2.0数据接口$音频支持=支持AAC/AAC+/AMR/MIDI/OGG/FLAC/WMA/WAV/APE/MP3等格式$视频支持=支持MP4/3GP/AVI等格式$图片支持=支持JPEG等格式$多媒体技术=HIFI音效（AK4375芯片）$常用功能=计算器，电子词典，电子书，闹钟，日历，录音机，情景模式，主题模式，收音机$商务功能=飞行模式，数据备份$服务特色=QQ客户端，新浪微博，微信，爱奇艺视频，腾讯视频，网易云音乐，高德地图，百度搜索，今日头条，电子书，美团，唯品会，大众点评，携程，支付宝，云服务$其他功能参数=分屏多任务$包装清单=主机&nbsp;x1 耳机&nbsp;x1 充电器&nbsp;x1 数据线&nbsp;x1 取卡针&nbsp;x1 透明后盖保护壳&nbsp;x1 保修卡&nbsp;x1 快速入门指南&nbsp;x1$保修政策=全国联保，享受三包服务$质保时间=1年$质保备注=主机1年，电池6个月，充电器1年，有线耳机3个月$客服电话=400-678-9688；800-830-5833$电话备注=全天24小时服务$详细内容=自购机日起（以购机发票为准），如因质量问题或故障，凭厂商维修中心或特约维修点的质量检测证明，享受7日内退货，30日内换货，30日以上在质保期内享受免费保修等三包服务！注：单独购买手机配件产品的用户，请完好保存配件外包装以及发票原件，如无法提供上述凭证的，将无法进行正常的配件保修或更换。进入官网&gt;&gt;";
		parser.parse(line2);
		Set<String> models = parser.getModels();
		System.out.println("==========结果:");
		for(String m : models){
			System.out.println(m);
		}
//		parseTest();
	}
}


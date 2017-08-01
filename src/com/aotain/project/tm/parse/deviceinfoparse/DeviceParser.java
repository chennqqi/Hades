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
 * dim_deviceinfo�������ն���Ϣ�����������ݽ���
 * @author Liangsj
 *
 */
public class DeviceParser {
	private String alias;// ����
	private String name;
	private String brand;
	private String type;
	private String price;
	private String deviceatt;
	private BrandConf brandConf;
	/** �Ƿ�����ɹ�*/
	private boolean parseSuccess = false;
	private static final String SPLIT = "\\|";
	
	private static final String TIME_REGES =  ".*��������=((200)|(19)).*";// ����ʱ��2010ǰ
	
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
	 * ��ȡ�����ͺ�
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
	 * ������Ч���ͺţ�����ȥ�����ַ��ͺ�
	 * @param models
	 * @return
	 */
	public Set<String> filter(Set<String> models){
		Set<String> result = new HashSet<String>();
		Set<String> sepSet = new HashSet<String>();
		for(String m : models){
			sepSet.add(StringUtil.rmSpecialChar(m));// ȥ�����ַ�
		}
		models.addAll(sepSet);
		for(String m : models){
			if(m.equals("MOBILE") || m.equals("BOX") || m.equals("TV") || m.equals("PAD")){
				System.out.println("��Ч:" + m);
				continue;
			}
			
			// �ͺŵ���Ʒ�Ƶģ���Ч
			List<Brand> k = brandConf.getFromAtts(Brand.EKEY, m);// TODO���Ż�����ȡ��Ʒ��
			List<Brand> n = brandConf.getFromAtts(Brand.ENAME, m);
			if((k != null && k.size() > 0)|| (n != null && n.size() > 0)){
				System.out.println("��Ч:" + m);
				continue;
			}
			result.add(m);
		}
		return result;
	}
	
	/**
	 * ���Ʋ��
	 *  ����GALAXY S7��G9300/ȫ��ͨ��
	 * 	a.ȥ���ż����ݣ�
			aa �滻����Ʒ��ΪӢ�ģ�ȥ�ո�
			bb ȥ���ģ�ȡʣ�²���
		b.ȡ��xxx/  ��xxx����Ϊ�ͺţ���������GB/4G/3G/2G/RAM
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
		
		// ��������ͺ�
		ModelHandle bracketModel = m.bracketModel();
		if(!StringUtil.isNull(bracketModel.getValue())){
			result.add(bracketModel.getValue());
			Brand b = brandConf.getFromOneAtt(Brand.CNAME, bracketModel.getBrand());
			if(b !=null){
				result.add(b.getEname() + bracketModel.getValue());// Ʒ��+�ͺ�
			}
		}
		
		return result;
	}
	
	/**
	 * �ӱ����л�ȡ�ͺ�
	 * A8000,����A8,������A8
	 * @return
	 */
	private Set<String> getModelsFromAlias(){
		Set<String> result = new HashSet<String>();
		if(StringUtil.isNull(alias)){
			return result;
		}
		String[] as = alias.split("[,��]");
		for(String a : as){
			ModelHandle m = new ModelHandle(brandConf, brand, a);
			m = m.removeSpace();
			result.addAll(getModelFromString(m));
		}
		return result;
	}
	
	
	/**
	 * ����һ���ͺ�
	 * 1���滻����Ʒ��ΪӢ��
	 * 2���滻����Ʒ��keyΪӢ�ģ�
	 * 3��12��ȥƷ��key��
	 * 4��12��ȥƷ�ƣ�
	 * 5��ȥkeyȥƷ��
	 * 6��ȥƷ�ơ��滻keyΪƷ��
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
	 * �۸��Ƿ�Ϸ�
	 * @return ���֣���x�򣬷���ture ,���򷵻�false(��δ���С������)
	 */
	public boolean isPriceLegal(){
		if(!parseSuccess || price.equals("")){
			return false;
		}
		price = price.replaceAll("��", "");
		try {
			Double.parseDouble(price);
		} catch (Exception e) {
			return false;
		}
		return true;
	}
	

	
	/**
	 * ����ʱ���Ƿ�2010��ǰ���ֻ�
	 * @return ��¼�޷���������true, ������Ϊ�ֻ���������ʱ����2010��ǰ������true,���򷵻�false
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
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("E:\\work\\dev\\6.ʡ����������Ŀ\\�ն�����\\device\\file\\all_20170710.txt"), "UTF-8"));
		PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream("E:\\work\\dev\\9.�ն˽���\\deviceInfoAnalysis\\data\\devicemodel.txt"), "UTF-8"));
		String line = null;
		while((line = br.readLine()) !=null){
			parser.parse(line);
			if(parser.parseSuccess && parser.isPriceLegal() && !parser.isPhoneBefore2010()){
				Set<String> models = parser.getModels();
				for(String m : models){
					String out = m + "|" + parser.getName() + "|" + parser.getBrand() + "|" + parser.getType() + "|";
					pw.println(out);
				}
				// SAMSUNGS7568|����S7568���ƶ�3G��|����|MOBILE|
			}
			pw.flush();
		}
		br.close();
		pw.close();
		System.out.println("��������");
	}
	
	public static void main(String[] args) throws Exception {
		BrandConf conf = new BrandConf();
		conf.load("config/brand_ch.csv");
//		conf.printAll();
		DeviceParser parser = new DeviceParser(conf);
		String line1 ="���� NOTE��С��note��Note 1S|С�׺���Note����ǿ��/�ƶ�3G/2GB RAM��|С��|MOBILE|2199|��������=2015��03��$�ֻ�����=4G�ֻ���3G�ֻ��������ֻ��������ֻ���ƽ���ֻ�������ֻ�$����������=����������㴥��$�����ߴ�=5.1Ӣ��$��������=Super AMOLED$�����ֱ���=2560x1440����$��Ļ�����ܶ�=576ppi$��Ļ����=˫����Ĵ����������ɲ���$խ�߿�=3.5mm$��Ļռ��=70.93%$4G����=�ƶ�TD-LTE����ͨTD-LTE����ͨFDD-LTE������TD-LTE������FDD-LTE$3G����=�ƶ�3G��TD-SCDMA��������3G��CDMA2000������ͨ3G��WCDMA������ͨ2G/�ƶ�2G��GSM��$֧��Ƶ��=2G��GSM B2/3/5/8 2G��CDMA 800 3G��CDMA EVDO 800 3G��WCDMA B1/2/5/8 3G��TD-SCDMA B34/39 4G��TD-LTE B38/39/40/41 4G��FDD-LTE B1/3/4/7/8/28$SIM��=˫����Nano SIM��$WLAN����=˫ƵWIFI��IEEE 802.11 a/b/g/n/ac$����=GPS������A-GPS������GLONASS��������������$�����빲��=NFC�����߳�磬����ң�أ�WLAN�ȵ㣬����4.1��OTG$����ϵͳ=Android 5.0$������=��˺�$CPU�ͺ�=���� Exynos 7420$CPUƵ��=2.1GHz�����ĺˣ���1.5GHz��С�ĺˣ�$GPU�ͺ�=Mali-T760$RAM����=3GB$ROM����=32GB$�洢��=��֧��������չ$�������=���ɲ�жʽ���$�������=2550mAh$���۴���ʱ��=Լ251Сʱ��2G+4G��$����Ӳ������=֧�ֿ��ٳ��$����ͷ����=˫����ͷ��ǰ��$��������ͷ=1600������$ǰ������ͷ=500������$�����=LED�����$��Ȧ=f/1.9$��Ƶ����=4K��3840x2160��30֡/�룩��Ƶ¼�� 1080p��1920��1080��30֡/�룩��Ƶ¼�� 720p��1280��720��30֡/�룩��Ƶ¼��$���չ���=��ʱ���ģ����ģ��Զ��Խ���OIS��ѧ������HDR����ʱ���գ��˾���ȫ�����գ����������춯������������$�������=ֱ��$������ɫ=�����ɫ��ѩ����ɫ��������ɫ�������ɫ$�ֻ��ߴ�=143.4x70.5x6.8mm$�ֻ�����=140g$�������=��������$��������=������$��Ӧ������=������Ӧ�������ߴ����������봫������ָ��ʶ�𣬼��ٴ����������ʴ�����$ָ��ʶ�����=ǰ��ָ��ʶ��$����ӿ�=3.5mm�����ӿڣ�Micro USB v2.0���ݽӿ�$������۲���=����չ����ܵĺ��$��Ƶ֧��=֧��MP3/WAV/eAAC+/AC3/FLAC�ȸ�ʽ$��Ƶ֧��=֧��MP4/DivX/XviD/WMV/H.264/H.263�ȸ�ʽ$ͼƬ֧��=֧��JPEG/PNG/GIF/BMP�ȸ�ʽ$���ù���=������������¼���ճ̱������飬���ӣ�������¼�������龰ģʽ������ģʽ����ͼ���$������=����ģʽ�����ݱ��ݣ����ݼ���$��װ�嵥=����&nbsp;x1 ������&nbsp;x1 ����&nbsp;x1 �����&nbsp;x1 ˵����&nbsp;x1$��������=ȫ��������������������$�ʱ�ʱ��=1��$�ʱ���ע=����1�꣬�����1�꣬���߶���3����$�ͷ��绰=400-810-5858$�绰��ע=��һ�����壺8:00-20:00�����������գ�8:00-17:00�����߷���$��ϸ����=�Թ��������Թ�����ƱΪ׼�������������������ϣ�ƾ����ά�����Ļ���Լά�޵���������֤��������7�����˻���15���ڻ�����15���������ʱ�����������ѱ��޵���������ע�����������ֻ������Ʒ���û�������ñ���������װ�Լ���Ʊԭ�������޷��ṩ����ƾ֤�ģ����޷�����������������޻�������������&gt;&gt;";
		String line2 ="P7-L09|��ΪAscend P7��P7-L09/����4G��|��Ϊ|MOBILE|1250|��������=2015��07��$�ֻ�����=4G�ֻ���3G�ֻ��������ֻ��������ֻ���ƽ���ֻ�$����������=����������㴥��$�����ߴ�=5.7Ӣ��$��������=Super AMOLED$�����ֱ���=1920x1080����$��Ļ�����ܶ�=386ppi$խ�߿�=2.86mm$��Ļռ��=74.05%$4G����=�ƶ�TD-LTE����ͨTD-LTE����ͨFDD-LTE������TD-LTE������FDD-LTE$3G����=�ƶ�3G��TD-SCDMA������ͨ3G��WCDMA��������3G��CDMA2000������ͨ2G/�ƶ�2G��GSM��$֧��Ƶ��=2G��CDMA 800 2G��GSM 850/900/1800/1900 3G��CDMA EVDO 800 3G��WCDMA 850/900/1900/2100 3G��TD-SCDMA B34/39 4G��TD-LTE 1900/2300/2600/2555-2575/2575-2635/2635-2655 4G��FDD-LTE 1750-1765/1765-1780/2100$SIM��=˫����Nano SIM��$WLAN����=˫ƵWIFI��IEEE 802.11 a/b/g/n$����=GPS������A-GPS������GLONASS��������������$�����빲��=WALN�ȵ㣬����4.1��NFC$����ϵͳ=Android 5.1$�û�����=Touch Wiz2015$������=��˺�$CPU�ͺ�=��ͨ ����615��MSM8939��$CPUƵ��=1.5GHz�����ĺˣ���1.0GHz��С�ĺˣ�$GPU�ͺ�=��ͨ Adreno405$RAM����=2GB$ROM����=16GB/32GB$�洢��=MicroSD��$��չ����=128GB$�������=���ɲ�жʽ���$�������=3050mAh$����ͷ����=˫����ͷ��ǰ��$��������ͷ=1600������$ǰ������ͷ=500������$�����=LED�����$��Ȧ=f/1.9$��Ƶ����=1080p��1920��1080��30֡/�룩��Ƶ¼��$���չ���=�й�ȡ���ƽ�⡢�ع⣬�Զ��Խ�������佹������ģ�����$�������=ֱ��$������ɫ=ħ�ý�ѩ��ף������$�ֻ��ߴ�=158x76.8x5.9mm$�ֻ�����=151g$�������=��������$��������=������$��Ӧ������=������Ӧ�������ߴ����������봫������ָ��ʶ��$ָ��ʶ�����=ǰ��ָ��ʶ��$����ӿ�=3.5mm�����ӿڣ�Micro USB v2.0���ݽӿ�$��Ƶ֧��=֧��MIDI/MP3/AAC�ȸ�ʽ$��Ƶ֧��=֧��MP4/3GP/AVC/AVI/MPEG-4�ȸ�ʽ$ͼƬ֧��=֧��JPEG/PNG/GIF/BMP�ȸ�ʽ$���ù���=��������������Ӵʵ䣬����¼���ճ̱����±�$������=ɧ�����أ�������ɱ��Ȩ�޹����������$��������=ȫ��������������������$�ʱ�ʱ��=1��$�ʱ���ע=����1�꣬�����1�꣬���߶���3����$�ͷ��绰=400-810-5858$�绰��ע=��һ�����壺8:00-20:00�����������գ�8:00-17:00�����߷���$��ϸ����=�Թ��������Թ�����ƱΪ׼�������������������ϣ�ƾ����ά�����Ļ���Լά�޵���������֤��������7�����˻���15���ڻ�����15���������ʱ�����������ѱ��޵���������ע�����������ֻ������Ʒ���û�������ñ���������װ�Լ���Ʊԭ�������޷��ṩ����ƾ֤�ģ����޷�����������������޻�������������&gt;&gt;";
		String line3 = "���Ű��Ŀ3D|HTC X515d ��Ŀ3D�����Ű棩|HTC|MOBILE|��Ϊ|MOBILE|2820|��������=2016��04��$��";
		String line4 = "CUBE U23GT ����(16G), ���ħ�� U23GT ����(16G)|���ħ��U23GT���� 16G|���ħ��|PAD|2��|��������=2015��12��$�ֻ�����=4G�ֻ���3G�ֻ��������ֻ��������ֻ��������ֻ�������ֻ�$����������=����������㴥��$�����ߴ�=5.2Ӣ��$��������=Super AMOLED$�����ֱ���=1920x1080����$��Ļ�����ܶ�=424ppi$խ�߿�=4.5mm$��Ļռ��=68.33%$������Ļ����=2.5D������$4G����=�ƶ�TD-LTE����ͨTD-LTE����ͨFDD-LTE$3G����=�ƶ�3G��TD-SCDMA������ͨ3G��WCDMA������ͨ2G/�ƶ�2G��GSM��$֧��Ƶ��=2G��GSM 850/900/1800/1900 3G��WCDMA 850/900/1900/2100 3G��TD-SCDMA 1880/2010 4G��TD-LTE B38/39/40/41 4G��FDD-LTE B1/3$SIM��=˫����Micro SIM��/Nano SIM��$WLAN����=��ƵWIFI��IEEE 802.11 b/g/n$����=GPS����$�����빲��=WLAN�ȵ㣬����4.0��OTG$����ϵͳ=Android 5.1$�û�����=Funtouch OS 2.5$������=��˺�$CPUƵ��=1.7GHz$GPU�ͺ�=Mali-T760$RAM����=4GB$ROM����=32GB$�洢��=MicroSD��$��չ����=128GB$�������=���ɲ�жʽ���$�������=2400mAh$����Ӳ������=˫��������$����ͷ����=˫����ͷ��ǰ��$��������ͷ=1300������$ǰ������ͷ=800������$����������=CMOS$�����=LED�����$��Ȧ=f/2.2$����ͷ��ɫ=����ʽ��ͷ$��Ƶ����=1080p��1920��1080��30֡/�룩��Ƶ¼��$���չ���=PDAF��λ�Խ�������׷��������Ӱ��ָ�����գ��Զ��Խ�������ͷ���쾵ͷ��HDR��ȫ��ģʽ��ҹ��ģʽ�����廭�ʣ��ĵ��������˶�׷�٣���ͯģʽ��רҵ���գ�רҵ¼��Ȥζģʽ����ױ���Ա�ʶ��$�������=ֱ��$������ɫ=��ɫ����ɫ��õ���$�ֻ��ߴ�=147.9x73.75x6.56mm$�ֻ�����=135.5g$�������=��������$��������=���ذ���$��Ӧ������=������Ӧ�������ߴ����������봫������ָ��ʶ��������$ָ��ʶ�����=����ָ��ʶ��$����ӿ�=3.5mm�����ӿڣ�Micro USB v2.0���ݽӿ�$��Ƶ֧��=֧��AAC/AAC+/AMR/MIDI/OGG/FLAC/WMA/WAV/APE/MP3�ȸ�ʽ$��Ƶ֧��=֧��MP4/3GP/AVI�ȸ�ʽ$ͼƬ֧��=֧��JPEG�ȸ�ʽ$��ý�弼��=HIFI��Ч��AK4375оƬ��$���ù���=�����������Ӵʵ䣬�����飬���ӣ�������¼�������龰ģʽ������ģʽ��������$������=����ģʽ�����ݱ���$������ɫ=QQ�ͻ��ˣ�����΢����΢�ţ���������Ƶ����Ѷ��Ƶ�����������֣��ߵµ�ͼ���ٶ�����������ͷ���������飬���ţ�ΨƷ�ᣬ���ڵ�����Я�̣�֧�������Ʒ���$�������ܲ���=����������$��װ�嵥=����&nbsp;x1 ����&nbsp;x1 �����&nbsp;x1 ������&nbsp;x1 ȡ����&nbsp;x1 ͸����Ǳ�����&nbsp;x1 ���޿�&nbsp;x1 ��������ָ��&nbsp;x1$��������=ȫ��������������������$�ʱ�ʱ��=1��$�ʱ���ע=����1�꣬���6���£������1�꣬���߶���3����$�ͷ��绰=400-678-9688��800-830-5833$�绰��ע=ȫ��24Сʱ����$��ϸ����=�Թ��������Թ�����ƱΪ׼�������������������ϣ�ƾ����ά�����Ļ���Լά�޵���������֤��������7�����˻���30���ڻ�����30���������ʱ�����������ѱ��޵���������ע�����������ֻ������Ʒ���û�������ñ���������װ�Լ���Ʊԭ�������޷��ṩ����ƾ֤�ģ����޷�����������������޻�������������&gt;&gt;";
		parser.parse(line1);
		Set<String> models = parser.getModels();
		System.out.println("==========���:");
		for(String m : models){
			System.out.println(m);
		}
		parseTest();
	}
}


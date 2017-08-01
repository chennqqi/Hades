package com.aotain.project.tm.parse.match;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.aotain.project.tm.parse.common.conf.BrandConf;
import com.aotain.project.tm.parse.common.conf.ModelMapping;
import com.aotain.project.tm.parse.common.pojo.Brand;
import com.aotain.project.tm.parse.common.pojo.Device;
import com.aotain.project.tm.parse.common.pojo.DeviceType;
import com.aotain.project.tm.parse.common.pojo.MatchEnum;
import com.aotain.project.tm.parse.common.pojo.MatchResult;
import com.aotain.project.tm.parse.common.utils.StringUtil;

/**
 * unchecked���ݽ�����
 * ��unchecked���ݽ��н�������ƥ���ն��ͺ�
 * @author Liangsj
 *
 */
public class UncheckParser {

	private String username;
	private String type;
	private String brand;
	private Brand pBrand;// ��������Ʒ��
	private String model;
	private String freq;
	private String date;
	private static final String SPLIT = ",";
	private boolean parseSuccess = false;
	private ModelMapping modelMapping = null;
	private BrandConf brandConf = null;
	
	public UncheckParser(ModelMapping modelMapping, BrandConf brandConf){
		this.modelMapping = modelMapping;
		this.brandConf = brandConf;
	}
	
	/**
	 * ����һ��
	 * ���ݸ�ʽ��
	 * 075500040608@163.gd,MOBILE,SAMSUNG#SM-A3000_4,1,20170729,
	 * @param line
	 */
	public void parse(String line){
		String[] arr = line.split(SPLIT);
		if(arr.length < 5){
			parseSuccess = false;
			return;
		}
		try {
			username = arr[0];
			type = arr[1];
			brand = arr[2].split("#")[0].toUpperCase();
			model = arr[2].split("#")[1].toUpperCase();
			freq = arr[3];
			parseSuccess = true;
		} catch (Exception e) {
			parseSuccess = false;
			return;
		}
	}
	
	public void parseTest(String line){
		String[] arr = line.split("	");
		if(arr.length < 2){
			parseSuccess = false;
			return;
		}
		try {
			type = arr[0];
			brand = arr[1].split("#")[0].toUpperCase();
			model = arr[1].split("#")[1].toUpperCase();
			parseSuccess = true;
		} catch (Exception e) {
			parseSuccess = false;
			return;
		}
	}

	/**
	 * ƥ��
	 * @return
	 */
	public MatchResult match(){
		if(!parseSuccess){
			return null;
		}
		/** ���ͺ�����ȡ��Ʒ�� */
		if(StringUtil.isNull(brand)){
			pBrand = parseBrand(model);
			System.out.println("���ͺ�����ȡ��Ʒ�ƣ�" + pBrand);
		} else {
			// Ʒ��תΪ����
			pBrand = new Brand();
			pBrand.setEname(brand);
			Brand b = brandConf.getFromOneAtt(Brand.ENAME, brand);
			System.out.println("Ʒ��תΪ����:" + b);
			if(b != null){
				pBrand.setCname(b.getCname());
			}
		}
		String modelNoSpace = StringUtil.rmSpace(model);
		
		
		if(null != pBrand){
			String modelNoBrand = modelNoSpace.replace(pBrand.getEname(), "");
			String modelNoKey = brandConf.rmEKey(modelNoSpace, pBrand.getCname());
			System.out.println("ȥƷ��:" + modelNoBrand);
			System.out.println("ȥƷ�ƹؼ���:" + modelNoKey);
			
			
			// ȡ��ӦƷ�Ƶ��ն�����
			List<Device> devices  = null;
			if(null != type && !type.equals(DeviceType.UNKNOWN.getName())) {
				devices = modelMapping.getDevice(type,pBrand.getCname());
			} else {
				devices = modelMapping.getDeviceFromBrand(pBrand.getCname());
			}
			if(null == devices){
				return null;
			}
			
			/** ȫƥ�� */
			Device result = wholeMatchOne(modelNoSpace, devices);
			if(result !=null){
				System.out.println("ȫƥ��:" + result);
				return new MatchResult(result, MatchEnum.WHOLE);
			}
			/** modelȥƷ�� */
			if(!modelNoBrand.trim().equals(modelNoSpace.trim())){
				result = match(modelNoBrand, devices);
				if(result !=null) {
					System.out.println("modelȥƷ��ƥ��:" + result);
					return new MatchResult(result, MatchEnum.WHOLE);
				}
			}
			
			/** modelȥƷ�ƹؼ��� */
			if(!modelNoKey.trim().equals(modelNoSpace.trim())){
				result = match(modelNoKey, devices);
				if(result !=null) {
					System.out.println("modelȥƷ�ƹؼ���ƥ��:" + result);
					return new MatchResult(result, MatchEnum.WHOLE);
				}
			}
			/** modelȡ�ؼ���Ϣ */
		}
		
		/**
		 * ֻ�����ͺ�ƥ��
		 */
		else if(modelNoSpace.length() >= 3){
			MatchEnum m = MatchEnum.MODEL;
			if(modelNoSpace.length() == 3){
				m = MatchEnum.MODEL_MIN;
			}
			List<Device> ds = wholeMatchAll(modelNoSpace, modelMapping.getDeviceList());
			System.out.println("ds:" + ds);
			if(ds.size() == 1){
				System.out.println("ֻƥ���ͺ�:" + ds.get(0));
				return new MatchResult(ds.get(0), m);
			} else if(ds.size() > 1){
				Set<String> brands = new HashSet<String>();
				for(Device d : ds){
					brands.add(d.getBrand());
				}
				// Ʒ��ֻ��һ�����ŷ���
				if(brands.size() ==1){
					return new MatchResult(ds.get(0), m);
				}
			}
		}
		return null;
	} 
	
	public Device match(String model, List<Device> devices){
		Device result = wholeMatchOne(model, devices);
		if(result ==null){
			String modelNoSpe = StringUtil.rmSpecialChar(model);
			if(!modelNoSpe.trim().equals(model.trim())) {
				System.out.println("ȥ�����ַ�:" + model);
				result = wholeMatchOne(modelNoSpe, devices);
			}
		}
		return result;
	}
	
	/**
	 * �����ͺ�ƥ���б����նˣ����ص�һ��ƥ����(equals)���ն�����
	 * @param model
	 * @param devices
	 * @return
	 */
	public Device wholeMatchOne(String model, List<Device> devices){
		System.out.println("ƥ��"+ model);
		if(null == devices || devices.size() == 0){
			return null;
		}
		for(Device d : devices){
			if(d.getModel().equals(model)){
				return d;
			}
		}
		return null;
	}
	
	public List<Device> wholeMatchAll(String model, List<Device> devices){
		if(null == devices || devices.size() == 0){
			return null;
		}
		List<Device> result = new ArrayList<Device>();
		for(Device d : devices){
			if(d.getModel().equals(model)){
				result.add(d);
			}
		}
		return result;
	}
	
	/**
	 * ��ȡƷ��(Ӣ��)
	 * @return
	 */
	private Brand parseBrand(String model){
		List<Brand> brands = brandConf.getBrands();
		for(Brand b : brands){
			if(b.getEkey().length() >= 4 && model.contains(b.getEkey())){
				return b;
			}
			if(b.getEname().length() >= 4 && model.contains(b.getEname())){
				return b;
			}
			if(model.startsWith(b.getEkey()+"-") || model.startsWith(b.getEkey()+" ") ){
				return b;
			}
		}
		return null;
	}
	
	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getBrand() {
		return brand;
	}

	public void setBrand(String brand) {
		this.brand = brand;
	}

	public String getModel() {
		return model;
	}

	public void setModel(String model) {
		this.model = model;
	}

	public String getFreq() {
		return freq;
	}

	public void setFreq(String freq) {
		this.freq = freq;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public boolean isParseSuccess() {
		return parseSuccess;
	}

	public void setParseSuccess(boolean parseSuccess) {
		this.parseSuccess = parseSuccess;
	}
	
	public Brand getpBrand() {
		return pBrand;
	}

	public void setpBrand(Brand pBrand) {
		this.pBrand = pBrand;
	}

	public static void testFile() throws Exception{
		BrandConf conf = new BrandConf();
		conf.load("config/brand_ch.csv");
		ModelMapping m = new ModelMapping();
		m.load("E:\\work\\dev\\9.�ն˽���\\deviceInfoAnalysis\\data\\devicemodel.txt");
		UncheckParser parser = new UncheckParser(m, conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("E:\\work\\dev\\9.�ն˽���\\deviceInfoAnalysis\\data\\model_export_new.txt"), "UTF-8"));
		PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream("E:\\work\\dev\\9.�ն˽���\\deviceInfoAnalysis\\data\\model_parse_min.txt"), "UTF-8"));
		String line = null;
		int count = 0;
		int pcount = 0;
		while((line = br.readLine()) !=null){
			if(count++ > 10000){
				break;
			}
			parser.parseTest(line);
			if(parser.isParseSuccess()){
				MatchResult d = parser.match();
				String out = "";
				if(d == null){
					out = parser.getType() + "|" + parser.getBrand() + "|"+ parser.getModel();
				} else {
					out = parser.getType() + "|" + parser.getBrand() + "|" + parser.getModel() + "     ====>    " + d.getDevice().getType() + "|" + d.getDevice().getBrand() + "|" + d.getDevice().getName();
					System.out.println(out);
					pcount++;
					if(d.getMatchType() == MatchEnum.MODEL_MIN)
						pw.println(out);
				}
//				pw.println(out);
			}
		}
		pw.flush();
		br.close();
		pw.close();
		System.out.println("��������,����" + count + ",������"+ pcount);
	}
	
	public static void main(String[] args) throws Exception {
		BrandConf conf = new BrandConf();
		conf.load("config/brand_ch.csv");
		ModelMapping m = new ModelMapping();
		m.load("E:\\work\\dev\\9.�ն˽���\\deviceInfoAnalysis\\data\\devicemodel.txt");
		UncheckParser p = new UncheckParser(m, conf);
		String t1 = "075500074358@163.gd,UNKNOWN,VIVO#VIVO X7PLUS,10,20170722,";
		String t2 = "075500080463@163.gd,MOBILE,NOKIA#N70,11,20170722,";
		p.parse(t2);
		System.out.println("���:" + p.match());
		System.out.println(p.getpBrand());
		System.out.println(p.getModel());
//		testFile();
		System.out.println("sdfc,��df".split("[,��]").length);
	}
}

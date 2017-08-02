package com.aotain.project.tm.parse.match;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.List;

import com.aotain.project.tm.parse.common.conf.BrandConf;
import com.aotain.project.tm.parse.common.conf.ModelMapping;
import com.aotain.project.tm.parse.common.pojo.Brand;
import com.aotain.project.tm.parse.common.pojo.MatchResult;
import com.aotain.project.tm.parse.common.utils.StringUtil;

/**
 * unchecked数据解析器
 * 对unchecked数据进行解析，并匹配终端型号
 * 输入：unchecked数据，型号-终端配置数据，品牌配置数据
 * @author Liangsj
 *
 */
public class UncheckParser {

	private String username;
	private String type;
	private String brand;
	private Brand pBrand;// 解析出的品牌
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
	 * 解析一行
	 * 数据格式：
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
	 * 匹配
	 * @return
	 */
	public MatchResult match(){
		if(!parseSuccess){
			return null;
		}
		/** 从型号中提取出品牌 */
		if(StringUtil.isNull(brand)){
			pBrand = parseBrand(model);
			System.out.println("从型号中提取出品牌：" + pBrand);
		} else {
			// 品牌转为中文
			pBrand = new Brand();
			pBrand.setEname(brand);
			Brand b = brandConf.getFromOneAtt(Brand.ENAME, brand, null);
			System.out.println("品牌转为中文:" + b);
			if(b != null){
				pBrand.setCname(b.getCname());
			}
		}
		
		// 匹配
		Matcher m  = new Matcher(pBrand, type,modelMapping, brandConf);
		return m.doMatch(model);
	} 
	
	/**
	 * 提取品牌
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
		m.load("E:\\work\\dev\\9.终端解析\\deviceInfoAnalysis\\data\\devicemodel.txt");
		UncheckParser parser = new UncheckParser(m, conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("E:\\work\\dev\\9.终端解析\\deviceInfoAnalysis\\data\\model_export_new.txt"), "UTF-8"));
		PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream("E:\\work\\dev\\9.终端解析\\deviceInfoAnalysis\\data\\model_parse_uncheck.txt"), "UTF-8"));
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
					pw.println(out);
				} else {
//					out = parser.getType() + "|" + parser.getBrand() + "|" + parser.getModel() + "     ====>    " + d.getDevice().getType() + "|" + d.getDevice().getBrand() + "|" + d.getDevice().getName();
					System.out.println(out);
					pcount++;
					/*if(d.getMatchType() == MatchEnum.MODEL_MIN)*/
//						pw.println(out);
				}
				
			}
		}
		pw.flush();
		br.close();
		pw.close();
		System.out.println("解析结束,共：" + count + ",解析出"+ pcount);
	}
	
	public static void main(String[] args) throws Exception {
		BrandConf conf = new BrandConf();
		conf.load("config/brand_ch.csv");
		ModelMapping m = new ModelMapping();
		m.load("E:\\work\\dev\\9.终端解析\\deviceInfoAnalysis\\data\\devicemodel.txt");
		UncheckParser p = new UncheckParser(m, conf);
		String t1 = "075500074358@163.gd,UNKNOWN,VIVO#VIVO X7PLUS,10,20170722,";
		String t2 = "075500080463@163.gd,MOBILE,NOKIA#N70,11,20170722,";
		p.parse(t2);
		System.out.println("结果:" + p.match());
		System.out.println(p.getpBrand());
		System.out.println(p.getModel());
//		testFile();
		System.out.println("sdfc,，df".split("[,，]").length);
	}
}

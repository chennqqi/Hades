package com.aotain.project.tm.parse.common.core;

import com.aotain.project.tm.parse.common.conf.BrandConf;
import com.aotain.project.tm.parse.common.utils.StringUtil;

/**
 * 型号处理
 * 对终端名称或别名，做相应处理
 * 如去空格，去品牌，去中文等
 * @author Liangsj
 *
 */
public class ModelHandle {
	private BrandConf brandConf;
	private String brand;
	private String value;
	
	private static final String MODEL_REGES =  "[（\\(]([-a-zA-Z\\d\\s\\+]*)\\/";// 取（到/部分 ，括号里的型号
	private static final String REMOVE_BRACKETS_REGES =  "(.*)[（\\(].*";// 去括号
	
	public ModelHandle(){}
	
	public ModelHandle(BrandConf brandConf, String brand, String value){
		this.brandConf = brandConf;
		this.value = value;
		this.brand = brand;
	}
	
	public ModelHandle(String value){
		this.value = value;
	}
	
	/**
	 * 去空格
	 * @return
	 */
	public ModelHandle removeSpace(){
		String newValue = StringUtil.rmSpace(value);
		return new ModelHandle(brandConf, brand,  newValue);
	}
	
	/**
	 * 去括号及其内容
	 * @return
	 */
	public ModelHandle removeBracket(){
		String newValue = StringUtil.findByRegex(value, REMOVE_BRACKETS_REGES, 1);
		if(newValue == null){
			newValue = value;
		}
//		System.out.println("去括号：" + newValue);
		return new ModelHandle(brandConf,  brand, newValue);
	}
	
	/**
	 * 去中文
	 * @return
	 */
	public ModelHandle removeChinese(){
		if(StringUtil.isNull(value)){
			return this;
		}
		String newValue = StringUtil.rmChinese(value).trim();
//		System.out.println("去中文：" +newValue);
		return new ModelHandle(brandConf, brand,  newValue);
	}
	
	/**
	 * 去品牌关键字
	 * @param brand
	 * @return
	 */
	public ModelHandle removeBrandKey(){
		if(StringUtil.isNull(value)){
			return this;
		}
		String newValue = brandConf.rmKey(value, brand);
//		System.out.println("去品牌关键字：" +newValue);
		return new ModelHandle(brandConf, brand,  newValue);
	}
	
	/**
	 * 替换中文品牌为英文品牌,中文key为英文key,且去除其它中文
	 * @return
	 */
	public ModelHandle replaceChinese(){
		if(StringUtil.isNull(value)){
			return this;
		}
		String newValue = StringUtil.rmChinese(brandConf.replaceCkey2EKey(brandConf.replaceCname2Ename(value, brand), brand));
//		System.out.println("替换中文品牌和key为英文：" +newValue);
		return new ModelHandle(brandConf, brand,  newValue);
	}
	
	/**
	 * 替换英文Key为英文品牌
	 * @return
	 */
	public ModelHandle replaceKeyToBrand(){
		if(StringUtil.isNull(value)){
			return this;
		}
		String newValue = brandConf.replaceEkey2EName(value, brand);
		return new ModelHandle(brandConf, brand,  newValue);
	}
	
	/**
	 * 去品牌(中英文)
	 * @param brand
	 * @return
	 */
	public ModelHandle removeBrand(){
		if(StringUtil.isNull(value)){
			return this;
		}
		String newValue = brandConf.rmBrand(value, brand);
//		System.out.println("去品牌：" +newValue);
		return new ModelHandle(brandConf, brand,  newValue);
	}
	
	/**
	 * 去品牌(中英文)
	 * @param brand
	 * @return
	 */
	public ModelHandle removeEbrand(){
		if(StringUtil.isNull(value)){
			return this;
		}
		String newValue = brandConf.rmEbrand(value, brand);
//		System.out.println("去品牌：" +newValue);
		return new ModelHandle(brandConf, brand,  newValue);
	}
	
	/**
	 * 去品牌(中英文)
	 * @param brand
	 * @return
	 */
	public ModelHandle removeCbrand(){
		if(StringUtil.isNull(value)){
			return this;
		}
		String newValue = brandConf.rmCbrand(value, brand);
//		System.out.println("去品牌：" +newValue);
		return new ModelHandle(brandConf, brand,  newValue);
	}

	/**
	 * 取括号里的型号
	 * 如 三星GALAXY A8(A8000/全网通)
	 * @return 如 三星GALAXY A8(A8000/全网通)，返回A8000
	 */
	public ModelHandle bracketModel(){
		if(StringUtil.isNull(value)){
			return this;
		}
		String newValue = StringUtil.findByRegex(value, MODEL_REGES, 1);
		if(isModel(newValue)){
//			System.out.println("括号里的型号：" +newValue);
			return new ModelHandle(brandConf, brand,  newValue);
		}
		return new ModelHandle(brandConf, brand,  null);
	}
	
	
	
	private boolean isModel(String model) {
		return model != null && !model.contains("GB") && !model.contains("4G") && !model.contains("3G") && !model.contains("2G");
	}
	
	public BrandConf getBrandConf() {
		return brandConf;
	}

	public void setBrandConf(BrandConf brandConf) {
		this.brandConf = brandConf;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
	
	
	public String getBrand() {
		return brand;
	}

	public void setBrand(String brand) {
		this.brand = brand;
	}

	public static void main(String[] args) {
		ModelHandle m = new ModelHandle("NUBIA M2 LITE");
		System.out.println(m.removeSpace().value);
	}
}

package com.aotain.project.tm.parse.common.core;

import com.aotain.project.tm.parse.common.conf.BrandConf;
import com.aotain.project.tm.parse.common.utils.StringUtil;

/**
 * �ͺŴ���
 * ���ն����ƻ����������Ӧ����
 * ��ȥ�ո�ȥƷ�ƣ�ȥ���ĵ�
 * @author Liangsj
 *
 */
public class ModelHandle {
	private BrandConf brandConf;
	private String brand;
	private String value;
	
	private static final String MODEL_REGES =  "[��\\(]([-a-zA-Z\\d\\s\\+]*)\\/";// ȡ����/���� ����������ͺ�
	private static final String REMOVE_BRACKETS_REGES =  "(.*)[��\\(].*";// ȥ����
	
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
	 * ȥ�ո�
	 * @return
	 */
	public ModelHandle removeSpace(){
		String newValue = StringUtil.rmSpace(value);
		return new ModelHandle(brandConf, brand,  newValue);
	}
	
	/**
	 * ȥ���ż�������
	 * @return
	 */
	public ModelHandle removeBracket(){
		String newValue = StringUtil.findByRegex(value, REMOVE_BRACKETS_REGES, 1);
		if(newValue == null){
			newValue = value;
		}
//		System.out.println("ȥ���ţ�" + newValue);
		return new ModelHandle(brandConf,  brand, newValue);
	}
	
	/**
	 * ȥ����
	 * @return
	 */
	public ModelHandle removeChinese(){
		if(StringUtil.isNull(value)){
			return this;
		}
		String newValue = StringUtil.rmChinese(value).trim();
//		System.out.println("ȥ���ģ�" +newValue);
		return new ModelHandle(brandConf, brand,  newValue);
	}
	
	/**
	 * ȥƷ�ƹؼ���
	 * @param brand
	 * @return
	 */
	public ModelHandle removeBrandKey(){
		if(StringUtil.isNull(value)){
			return this;
		}
		String newValue = brandConf.rmKey(value, brand);
//		System.out.println("ȥƷ�ƹؼ��֣�" +newValue);
		return new ModelHandle(brandConf, brand,  newValue);
	}
	
	/**
	 * �滻����Ʒ��ΪӢ��Ʒ��,����keyΪӢ��key,��ȥ����������
	 * @return
	 */
	public ModelHandle replaceChinese(){
		if(StringUtil.isNull(value)){
			return this;
		}
		String newValue = StringUtil.rmChinese(brandConf.replaceCkey2EKey(brandConf.replaceCname2Ename(value, brand), brand));
//		System.out.println("�滻����Ʒ�ƺ�keyΪӢ�ģ�" +newValue);
		return new ModelHandle(brandConf, brand,  newValue);
	}
	
	/**
	 * �滻Ӣ��KeyΪӢ��Ʒ��
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
	 * ȥƷ��(��Ӣ��)
	 * @param brand
	 * @return
	 */
	public ModelHandle removeBrand(){
		if(StringUtil.isNull(value)){
			return this;
		}
		String newValue = brandConf.rmBrand(value, brand);
//		System.out.println("ȥƷ�ƣ�" +newValue);
		return new ModelHandle(brandConf, brand,  newValue);
	}
	
	/**
	 * ȥƷ��(��Ӣ��)
	 * @param brand
	 * @return
	 */
	public ModelHandle removeEbrand(){
		if(StringUtil.isNull(value)){
			return this;
		}
		String newValue = brandConf.rmEbrand(value, brand);
//		System.out.println("ȥƷ�ƣ�" +newValue);
		return new ModelHandle(brandConf, brand,  newValue);
	}
	
	/**
	 * ȥƷ��(��Ӣ��)
	 * @param brand
	 * @return
	 */
	public ModelHandle removeCbrand(){
		if(StringUtil.isNull(value)){
			return this;
		}
		String newValue = brandConf.rmCbrand(value, brand);
//		System.out.println("ȥƷ�ƣ�" +newValue);
		return new ModelHandle(brandConf, brand,  newValue);
	}

	/**
	 * ȡ��������ͺ�
	 * �� ����GALAXY A8(A8000/ȫ��ͨ)
	 * @return �� ����GALAXY A8(A8000/ȫ��ͨ)������A8000
	 */
	public ModelHandle bracketModel(){
		if(StringUtil.isNull(value)){
			return this;
		}
		String newValue = StringUtil.findByRegex(value, MODEL_REGES, 1);
		if(isModel(newValue)){
//			System.out.println("��������ͺţ�" +newValue);
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

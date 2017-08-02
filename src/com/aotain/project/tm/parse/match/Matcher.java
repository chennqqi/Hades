package com.aotain.project.tm.parse.match;

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
 * �ն�ƥ��
 * @author Administrator
 *
 */
public class Matcher {

	private Brand brand;// Ʒ��
	private String type;// ����
	
	private ModelMapping modelMapping = null;// �ͺ���������
	private BrandConf brandConf = null;// Ʒ����������
	
	private List<Device> devicesOfBrand;// ��Ʒ�Ƶ������ն�
	private List<Device> allDevices;// �����ն�
	
	public Matcher() {
		super();
	}

	public Matcher(Brand brand, String type, ModelMapping modelMapping, BrandConf brandConf) {
		super();
		this.brand = brand;
		this.type = type;
		this.modelMapping = modelMapping;
		this.brandConf = brandConf;
		init();
	}
	
	public void init(){
		// ȡ��ӦƷ�Ƶ��ն�����
		if(null != brand){
			if(null != type && !type.equals(DeviceType.UNKNOWN.getName())) {
				devicesOfBrand = modelMapping.getDevice(type,brand.getCname());
			} else {
				devicesOfBrand = modelMapping.getDeviceFromBrand(brand.getCname());
			}
		}
		allDevices = modelMapping.getDeviceList();
	}
	
	public MatchResult doMatch(String str){
		String modelNoSpace = StringUtil.rmSpace(str).trim();
		
		if(null != brand){
			if(null == devicesOfBrand){
				// ��Ʒ�ƣ����Ҳ���Ʒ����������
				return null;
			}
			
			/** ȫƥ�� */
			Device result = matchWithBrand(modelNoSpace);
			if(result !=null){
				System.out.println("ȫƥ��:" + result);
				return new MatchResult(result, MatchEnum.WHOLE);
			}
			/** modelȥƷ��ƥ�� */
			String modelNoBrand = modelNoSpace.replace(brand.getEname(), "").trim();
			System.out.println("ȥƷ��:" + modelNoBrand);
			if(!modelNoBrand.equals(modelNoSpace)){
				result = matchWithBrand(modelNoBrand);
				if(result !=null) {
					System.out.println("modelȥƷ��ƥ��:" + result);
					return new MatchResult(result, MatchEnum.WHOLE);
				}
			}
			
			/** modelȥƷ�ƹؼ���ƥ�� */
			String modelNoKey = brandConf.rmEKey(modelNoSpace, brand.getCname()).trim();
			System.out.println("ȥƷ�ƹؼ���:" + modelNoKey);
			if(!modelNoKey.equals(modelNoSpace)){
				result = matchWithBrand(modelNoKey);
				if(result !=null) {
					System.out.println("modelȥƷ�ƹؼ���ƥ��:" + result);
					return new MatchResult(result, MatchEnum.WHOLE);
				}
			}
			/** modelȡ�ؼ���Ϣ */
		}
		
		// û��Ʒ��ʱ��ֻ�����ͺ�ƥ��
		return onlyModelMath(modelNoSpace);
	}
	
	private MatchResult onlyModelMath(String model){
		if(model.length() >= 3){
			MatchEnum m = MatchEnum.MODEL;
			if(model.length() == 3){
				m = MatchEnum.MODEL_MIN;
			}
			List<Device> ds = wholeMatchAll(model, allDevices);
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
	
	/**
	 * ���ն��б����ҳ��ͺ�һ�����նˣ����Ҳ�����ȥ�����ַ�����ƥ��һ��
	 * @param model Ҫ���ҵ��ͺ�
	 * @param devices �ն��б�
	 * @return
	 */
	private Device matchWithBrand(String model){
		Device result = wholeMatchOne(model, devicesOfBrand);
		if(result ==null){
			String modelNoSpe = StringUtil.rmSpecialChar(model);
			if(!modelNoSpe.trim().equals(model.trim())) {
				System.out.println("ȥ�����ַ�:" + model);
				result = wholeMatchOne(modelNoSpe, devicesOfBrand);
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
	private Device wholeMatchOne(String model, List<Device> devices){
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
	
	/**
	 * ƥ��������ն�
	 * @param model
	 * @param devices
	 * @return
	 */
	private List<Device> wholeMatchAll(String model, List<Device> devices){
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
	
}

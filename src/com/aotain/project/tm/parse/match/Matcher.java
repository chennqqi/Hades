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
 * 终端匹配
 * @author Administrator
 *
 */
public class Matcher {

	private Brand brand;// 品牌
	private String type;// 类型
	
	private ModelMapping modelMapping = null;// 型号配置数据
	private BrandConf brandConf = null;// 品牌配置数据
	
	private List<Device> devicesOfBrand;// 该品牌的所有终端
	private List<Device> allDevices;// 所有终端
	
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
		// 取对应品牌的终端配置
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
				// 有品牌，但找不到品牌配置数据
				return null;
			}
			
			/** 全匹配 */
			Device result = matchWithBrand(modelNoSpace);
			if(result !=null){
				System.out.println("全匹配:" + result);
				return new MatchResult(result, MatchEnum.WHOLE);
			}
			/** model去品牌匹配 */
			String modelNoBrand = modelNoSpace.replace(brand.getEname(), "").trim();
			System.out.println("去品牌:" + modelNoBrand);
			if(!modelNoBrand.equals(modelNoSpace)){
				result = matchWithBrand(modelNoBrand);
				if(result !=null) {
					System.out.println("model去品牌匹配:" + result);
					return new MatchResult(result, MatchEnum.WHOLE);
				}
			}
			
			/** model去品牌关键字匹配 */
			String modelNoKey = brandConf.rmEKey(modelNoSpace, brand.getCname()).trim();
			System.out.println("去品牌关键字:" + modelNoKey);
			if(!modelNoKey.equals(modelNoSpace)){
				result = matchWithBrand(modelNoKey);
				if(result !=null) {
					System.out.println("model去品牌关键字匹配:" + result);
					return new MatchResult(result, MatchEnum.WHOLE);
				}
			}
			/** model取关键信息 */
		}
		
		// 没有品牌时，只根据型号匹配
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
				System.out.println("只匹配型号:" + ds.get(0));
				return new MatchResult(ds.get(0), m);
			} else if(ds.size() > 1){
				Set<String> brands = new HashSet<String>();
				for(Device d : ds){
					brands.add(d.getBrand());
				}
				// 品牌只有一个，才返回
				if(brands.size() ==1){
					return new MatchResult(ds.get(0), m);
				}
			}
		}
		return null;
	}
	
	/**
	 * 从终端列表中找出型号一样的终端，若找不到，去特殊字符，再匹配一次
	 * @param model 要查找的型号
	 * @param devices 终端列表
	 * @return
	 */
	private Device matchWithBrand(String model){
		Device result = wholeMatchOne(model, devicesOfBrand);
		if(result ==null){
			String modelNoSpe = StringUtil.rmSpecialChar(model);
			if(!modelNoSpe.trim().equals(model.trim())) {
				System.out.println("去特殊字符:" + model);
				result = wholeMatchOne(modelNoSpe, devicesOfBrand);
			}
		}
		return result;
	}
	
	/**
	 * 根据型号匹配列表中终端，返回第一个匹配上(equals)的终端名称
	 * @param model
	 * @param devices
	 * @return
	 */
	private Device wholeMatchOne(String model, List<Device> devices){
		System.out.println("匹配"+ model);
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
	 * 匹配出所有终端
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

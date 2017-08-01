package com.aotain.project.gdtelecom.ua.pojo;

/**
 * 正则配置里的属性
 * @author Administrator
 *
 */
public class DeviceProperties{

	private PropNameEnum key;
	private String value;

	public DeviceProperties() {
		super();
	}

	public DeviceProperties(String key, String value) {
		super();
		this.key = PropNameEnum.nameOf(key);
		this.value = value;
	}

	public DeviceProperties(String key) {
		super();
		this.key = PropNameEnum.nameOf(key);
	}


	public PropNameEnum getKey() {
		return key;
	}

	public void setKey(PropNameEnum key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "[" + key + "," + value + "]";
	}

}

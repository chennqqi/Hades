package dmpcommon.pojo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class DeviceRegex implements Serializable{

	private static final long serialVersionUID = 1L;
	private String regex; // UA串中，终端数据的正则表达式
	private List<DeviceProperties> props = new ArrayList<DeviceProperties>();
	
	public void addProp(DeviceProperties prop) {
		props.add(prop);
	}
	
	public String getRegex() {
		return regex;
	}
	
	public void setRegex(String regex) {
		this.regex = regex;
	}

	@Override
	public String toString() {
		String result = "";
		for(DeviceProperties prop : props) {
			result += prop;
		}
		return result;
	}

	public List<DeviceProperties> getProps() {
		return props;
	}

	public void setProps(List<DeviceProperties> props) {
		this.props = props;
	}
	
	
	
}

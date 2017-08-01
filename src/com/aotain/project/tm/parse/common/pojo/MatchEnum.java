package com.aotain.project.tm.parse.common.pojo;

public enum MatchEnum {
	MANUAL(0,"手工"),
	WHOLE(1, "全匹配"),
	MODEL(2,"型号匹配"),
	MODEL_PART(3, "型号部分匹配"),
	MODEL_MIN(4, "3位长度型号匹配");
	
	
    private int val;
    private String valInfo;

    MatchEnum(int val, String valInfo) {
        this.val = val;
        this.valInfo = valInfo;
    }

	public int getVal() {
		return val;
	}

	public void setVal(int val) {
		this.val = val;
	}

	public String getValInfo() {
		return valInfo;
	}

	public void setValInfo(String valInfo) {
		this.valInfo = valInfo;
	}
    
    
}

package com.aotain.project.tm.parse.common.pojo;

public enum MatchEnum {
	MANUAL(0,"�ֹ�"),
	WHOLE(1, "ȫƥ��"),
	MODEL(2,"�ͺ�ƥ��"),
	MODEL_PART(3, "�ͺŲ���ƥ��"),
	MODEL_MIN(4, "3λ�����ͺ�ƥ��");
	
	
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

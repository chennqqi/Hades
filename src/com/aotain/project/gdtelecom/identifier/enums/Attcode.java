package com.aotain.project.gdtelecom.identifier.enums;

public enum Attcode {
	QQACCOUNT("1", "QQAccount"), 
	MAIL("2", "Mail"), 
	IMEI("3", "IMEI"), 
	MAC_TERMINAL("4", "MAC_Terminal"), 
	PHONE("6", "Phone"), 
	IMSI("7", "IMSI"), 
	PHONE_TERMINAL("8", "Phone_Terminal"),
	TERMIAL("9", "Terminal"),
	IDFA("5", "IDFA"),
	PHONE_APP("10","Phone_App"),
	MAC_APP("11", "MAC_App"), 
	APP("12", "App"),
	PHONE_NOTKEY("13", "Phone_Notkey"),
	TERMIAL_BLOCK("14", "Terminal_Block");
	

	private String id;
	private String name;

	private Attcode(String id, String name) {
		this.id = id;
		this.name = name;
	}

	private String getId() {
		return id;
	}

	public String getName() {
		return name;
	}
	
	public static String nameof(String id) {
		for(Attcode idtype : values()){
            if(idtype.id.equals(id)){
                return idtype.getName();
            }
        }
		return null;
	}

}

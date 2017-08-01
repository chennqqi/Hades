package com.aotain.project.gdtelecom.ua.pojo;

public enum DeviceType
{

  MOBILE("MOBILE"), 

  PAD("PAD"), 

  TV("TV"), 
  
  BOX("BOX"), 

  UNKNOWN("UNKNOWN");

  String name;

  private DeviceType(String name) {
    this.name = name;
  }

  public String getName() {
    return this.name;
  }
  
  public static DeviceType nameOf(String name) {
	  for(DeviceType type : values()) {
		  if(type.getName().equalsIgnoreCase(name)) {
			  return type;
		  }
	  }
	  return UNKNOWN;
  }
}
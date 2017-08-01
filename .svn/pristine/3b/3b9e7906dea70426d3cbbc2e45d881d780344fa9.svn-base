package com.aotain.ods.ua;

public enum DeviceType
{
  COMPUTER("Computer"), 

  MOBILE("Mobile"), 

  TABLET("Tablet"), 

  GAME_CONSOLE("Game console"), 

  DMR("Digital media receiver"), 

  UNKNOWN("Unknown");

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
	  return null;
  }
}
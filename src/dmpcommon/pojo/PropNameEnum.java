package dmpcommon.pojo;

public enum PropNameEnum {
	VENDOR("VENDOR"), // 品牌
	MODEL("MODEL"), // 型号
	TYPE("TYPE"); // 类型

	private String name;

	private PropNameEnum(String name) {
		this.name = name;
	}

	public String getName() {
		return this.name;
	}
	
	 public static PropNameEnum nameOf(String name) {
		  for(PropNameEnum type : values()) {
			  if(type.getName().equalsIgnoreCase(name)) {
				  return type;
			  }
		  }
		  return null;
	  }
}

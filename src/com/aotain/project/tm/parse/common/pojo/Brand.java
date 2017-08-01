package com.aotain.project.tm.parse.common.pojo;

/**
 * 品牌
 * @author Liangsj
 *
 */
public class Brand {
	private String ckey;// 关键字，如三星的key有盖乐世
	private String ekey;// 关键字 英文名 如GALAXY
	private String cname;// 品牌中文名，如三星
	private String ename;// 品牌英文名，如SAMSUNG
	
	public static final int CKEY = 1;
	public static final int EKEY = 2;
	public static final int CNAME = 3;
	public static final int ENAME = 4;
	
	public Brand() {
		super();
	}
	public Brand(String ckey, String ekey,String cname, String ename) {
		super();
		this.ckey = ckey;
		this.ekey = ekey;
		this.cname = cname;
		this.ename = ename;
	}

	/**
	 * 根据index，返回相应的值
	 * @param index CKEY EKEY CNAME ENAME
	 * @return
	 */
	public String valueOf(int index){
		switch(index){
		case CKEY :
			return ckey;
		case EKEY :
			return ekey;
		case CNAME :
			return cname;
		case ENAME :
			return ename;
		default : 
			return null;
		}
	}

	public String getCkey() {
		return ckey;
	}
	public void setCkey(String ckey) {
		this.ckey = ckey;
	}
	public String getEkey() {
		return ekey;
	}
	public void setEkey(String ekey) {
		this.ekey = ekey;
	}
	public String getCname() {
		return cname;
	}
	public void setCname(String cname) {
		this.cname = cname;
	}
	public String getEname() {
		return ename;
	}
	public void setEname(String ename) {
		this.ename = ename;
	}
	@Override
	public String toString() {
		return "Brand [ckey=" + ckey + ", ekey=" + ekey + ", cname=" + cname + ", ename=" + ename + "]";
	}
	
	
}


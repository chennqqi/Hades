package com.aotain.project.tm.parse.common.pojo;

/**
 * Ʒ��
 * @author Liangsj
 *
 */
public class Brand {
	private String ckey;// �ؼ��֣������ǵ�key�и�����
	private String ekey;// �ؼ��� Ӣ���� ��GALAXY
	private String cname;// Ʒ����������������
	private String ename;// Ʒ��Ӣ��������SAMSUNG
	
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
	 * ����index��������Ӧ��ֵ
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


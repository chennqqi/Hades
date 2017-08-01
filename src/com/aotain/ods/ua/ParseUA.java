package com.aotain.ods.ua;

public class ParseUA {

	/**
	 * ��ȡϵͳ���汾���ն��ͺ��Լ�����
	 * @param UA
	 * @return
	 */
	public String getUAInfo(String UA){
		
		try
		{
			StringBuffer sb = new StringBuffer();
			if (UA == null || "".equals(UA)) {
				sb.append("").append("|").append("").append("|").append("");
				return sb.toString();
			}
			
			UserAgent ua = UserAgent.parseUserAgentString(UA);
			OperatingSystem os = ua.getOperatingSystem();
			
			String osName = os.getName();
			String osVer = os.getOsVersion();
			String deviceModel = "";
			ParseUA parseUA = new ParseUA();
			if (osName.toLowerCase().indexOf("unknown") == -1) 
				deviceModel = parseUA.getDeviceModel(UA);
			else 
				osName = "";
			if (osVer == null) 
				osVer = "";
			String deviceType = os.getDeviceType().getName().toLowerCase();
			int deviceTypeFlag = getDeviceType(deviceType,deviceModel);
			sb.append(os.getName()).append("|").append(os.getOsVersion()).append("|")
			.append(deviceModel).append("|").append(deviceTypeFlag);
			return sb.toString();
		}
		catch(Exception ex)
		{
			StringBuffer sb = new StringBuffer();
			sb.append("").append("|").append("").append("|").append("");
			return sb.toString();
		}
	}
	
	/**
	 * ��ȡ�ն��ͺ�
	 * @param UA
	 * @return
	 */
	public String getDeviceModel(String UA) {
		
		try
		{
		if (UA.indexOf("iPhone")!=-1){
			if (UA.indexOf("iPhone OS")!=-1 && UA.indexOf("like")!=-1 && UA.indexOf(";") != -1) 
				return UA.substring(UA.indexOf("iPhone OS"),UA.indexOf("like")) ;
				
			return "iPhone";	
		}
		
		if (UA.indexOf("Build")!=-1){
			UA = UA.substring(0,UA.indexOf("Build"));
			if (UA.indexOf(";") != -1) 
				return UA.substring(UA.lastIndexOf(";")+1,UA.length());
		}
		
		if (UA.indexOf("MIUI")!=-1) {
			UA = UA.substring(0,UA.indexOf("MIUI")+4);
			if (UA.indexOf(";") != -1) 
				return  UA.substring(UA.lastIndexOf(";")+1,UA.length());
		}
		
		return "";
		}
		catch(Exception ex)
		{
			return "";
		}
	}
	
	/**
	 * ��ȡ�ն��ͺ� 0-�ƶ���1-PC��2-δ֪
	 * @param UA
	 * @return
	 */
	public int getDeviceType(String deviceType,String deviceModel){
		
		if (deviceType.indexOf("mobile")!=-1
				||deviceType.indexOf("tablet")!=-1)
			return 0;
		else if(deviceType.indexOf("computer")!=-1) 
			return 1;
		else 
			return 2;
	
	}
}

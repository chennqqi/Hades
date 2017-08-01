package com.aotain.project.ecommerce;

import com.aotain.common.CommonFunction;

public class CodeUtils {

	public static String decode(String str) {
		if(!str.contains("%")){
			return str;
		}
		String code =  str.replaceAll("%(?![0-9a-fA-F]{2})", "%25");
		byte[] buf = CommonFunction.GetUrlCodingToBytes(code);
		String result = str;
		 try {
			 if(CommonFunction.IsUTF8(buf)){
				 result =java.net.URLDecoder.decode(code,"utf-8");
			}else{
				result = java.net.URLDecoder.decode(code,"gbk");
			}
		 } catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
}

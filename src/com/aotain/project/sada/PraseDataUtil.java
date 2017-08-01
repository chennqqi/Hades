package com.aotain.project.sada;

import com.aotain.common.CommonFunction;

public class PraseDataUtil {
	  public static String setChooseValue(String choosevalue,String choosecloums,String []items){
	    	String []choosevalues=choosevalue.split("\\|");
	    	int index=Integer.parseInt(choosevalues[0]);
	    	String isBase64=choosevalues[1];
	        if(isBase64.equals("t")){
	        	choosecloums = CommonFunction.decodeBASE64(items[index]);
	        }else{
	        	choosecloums = items[index];
	        }
	    	
	    	return choosecloums;
	    }
}

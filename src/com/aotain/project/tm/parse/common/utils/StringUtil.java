package com.aotain.project.tm.parse.common.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtil {
	
	public static boolean matcher(String timeRegex , String str){
		Pattern p = Pattern.compile(timeRegex);
		return p.matcher(str).find();
	}
	
	public static String findByRegex(String str, String regex, int group) {
		String resultValue = null;
		if ((str == null) || (regex == null) || ((regex != null) && ("".equals(regex.trim()))))
			return resultValue;

		Pattern p = Pattern.compile(regex);
		Matcher m = p.matcher(str);

		if (m.find()) {
			resultValue = m.group(group);
		}
		return resultValue;
	}
	
	public static boolean isNull(String str){
		return null == str || str.trim().equals("");
	}
	
	public static boolean contains(String src, String target){
		if(target == null || src == null || src.length() < 1 || target.length() < 1){
			return false;
		}
		return src.contains(target);
	}
	
	public static String rmChinese(String str){
		return str.replaceAll("[\u4e00-\u9fa5]", "");
	}

	public static String rmSpace(String str){
		return str.replaceAll("\\s", "");
	}
	
	/**
	 * ȥ�����ַ� +-_
	 * @param str
	 * @return
	 */
	public static String rmSpecialChar(String str){
		return str.replaceAll("[\\+\\-_]", "");
	}
	
	public static void main(String[] args) {
		System.out.println(StringUtil.rmSpace("SAMSUNGGALAXY A8"));
		System.out.println(StringUtil.contains("SAMSUNGGALAXY A8",""));
		System.out.println("8848 8848 aa".replaceFirst("8848", ""));
		System.out.println(StringUtil.rmSpecialChar("SAMSUNG-N9300"));
	}
}

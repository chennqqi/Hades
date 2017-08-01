package com.aotain.project.gdtelecom;


public class SimilarFunction {

	private static int getCommonStrLength(String str1, String str2) {
		str1 = str1.toLowerCase();
		str2 = str2.toLowerCase();
		int len1 = str1.length();
		int len2 = str2.length();
		String min = null;
		String max = null;
		String target = null;
		min = len1 <= len2 ? str1 : str2;
		max = len1 > len2 ? str1 : str2;
		for (int i = min.length(); i >= 1; i--) {
			for (int j = 0; j <= min.length() - i; j++) {
				target = min.substring(j, j + i);
				for (int k = 0; k <= max.length() - i; k++) {
					if (max.substring(k, k + i).equals(target)) {
						return i;
					}
				}
			}
		}
		return 0;
	}

	public static float Similar(String ua, String spider) {
		if (ua.length() == 0) return 0;
		return (float) getCommonStrLength(ua, spider) / ua.length();
	}
	
	public static void main(String[] args) {
		System.out.println(Similar("abc", "abccd"));	
		System.out.println(Similar("abc", "abc"));
		System.out.println(Similar("abc", "abd"));
//		System.out.println(getCommonStrLength("abc", "abd"));
	}
}

package com.aotain.project.gdtelecom.identifier.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {

	private static StringBuffer bs = new StringBuffer();
	private static final String REGEX_PHONE = "[;?&,}\\s*]{1}(\\w+)(:|=)([1][0-9]{10})[;&,}\\s*]{1}";
	private static final String REGEX_PHONE_C = "(=|:)([1][0-9]{10})[;&,}\\s*]{1}";
	public static void main(String[] args) throws UnsupportedEncodingException, ParseException {
		String cookie =  "__pad__figure__=1; _ga=ga1.2.112993080.1467177733; hm_lvt_53b7374a63c37483e5dd97d78d9bb36e=1467177734,1467290599; p00001=b8ppp6pj3m2qxnoqfwo3w2wyjxq2rfm3hm3xakjgyabcnybrrfym3tqkym1caom1xrpnuwlb36; p00002={pru:2010021890,uid:2010021890,type:11,pnickname:chiuwan,email:13512761248,user_name:13512761248,nickname:chiuwan}; p00003=2010021890; p00004=-1224347632.1461516193.f909b0dcf0; qc006=edusg67stdbfb79s4cg07722; qc008=1467177729.1467177729.1467290598.2; t00404=624ab8094c9ae4d27ad614a798d152bb";
		long start = System.currentTimeMillis();
		int i=0;
		while(i++<10000){
//			findByRegex(cookie, REGEX_PHONE_C, 2);
		}
		System.out.println(findByRegex(cookie, REGEX_PHONE, 2));
		System.out.println(System.currentTimeMillis() - start);
		System.out.println(findPhCount(cookie, REGEX_PHONE_C, 2));
		String[] items = "f42e9ce07dbc4fe410b811b60ac888ee,App,QQ浏览器,2,1,20161211".split(",", -1);
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        System.out.println(items[4]);
//        Date date1 = format.parse(items[4].substring(0, 8));
        System.out.println("");
        String[] arr = findAllByRegex(cookie, REGEX_PHONE);
        System.out.println("phone:"+arr[0]);
        for(String s : arr){
        	System.out.println(s);
        }
        String s = "苹果#苹果IPHONE 8（国际版/全网通）";
        String att = s.replaceAll("[\\s]", "");// 去空格
  		if(att.contains("IPHONE8")){
        s = s.replaceAll("苹果.*", "苹果#其他");
  		}
        System.out.println(s);
	}
	
	public static void decodefile(String filein, String fileout) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(filein));
		PrintWriter pw = new PrintWriter(fileout);
		String line = null;
		String out = null;
		while((line = br.readLine()) !=null) {
			out =decodepost(line);
			out = out.replaceAll("\\s", " ");
			pw.println(out);
			/*if(out.contains("<div")){
				System.out.println(line);
				break;
			}*/
		}
		pw.flush();
		pw.close();
		br.close();
	} 
	
	protected static String decode(String str) {
		byte[] bt = null;
		try {
			sun.misc.BASE64Decoder decoder = new sun.misc.BASE64Decoder();
			bt = decoder.decodeBuffer(str);
		} catch (IOException e) {
		}
		return new String(bt);
	}
	
	public static String decodepost(String str) throws UnsupportedEncodingException {
		str  = decode(str);
		if(str.contains("%")) {
			str = str.replaceAll("%(?![0-9a-fA-F]{2})", "%25");
			str = java.net.URLDecoder.decode(str, "utf-8");
		}
		return str;
	}

	public static void formate() {
		String s = String.format("%s,%s,%s,%s,%s,", "a", "a", "a", "a", "a");
	}

	public static void sb() {
		StringBuffer sb = new StringBuffer();
		sb.append("a").append("a").append("a").append("a").append("a").append("a").append("a").append("a").append("a")
				.append("a");
		String s = sb.toString();
	}

	public String parseTelTypeFromName(String name) {
		// Moto RAZR V锋芒（MT887/移动版）
		String regex1 = "（([-a-zA-Z\\d\\s\\+]*)\\/";// 取（到/部分
		String regex2 = "([-a-zA-Z\\d\\s\\+]+)[（]";
		String regex3 = "([-a-zA-Z\\d\\s\\+]+)";
		String result = find(name, regex1);
		if (!isModel(result)) {
			result = find(name, regex2);
		}
		if (!isModel(result)) {
			result = find(name, regex3);
		}
		if (!isModel(result)) {
			result = "";
		}
		return result.toUpperCase();

	}
	
	private static void contains(String str){
		str.contains("iPhone");
	}
	
	private static void match(String str) {
		Matcher pat = Pattern.compile("(iphone.*?)([^\\s\\.a-zA-Z0-9]|\\t|OS|$)", Pattern.CASE_INSENSITIVE).matcher(str);
		pat.find();
		
	}

	private boolean isModel(String model) {
		return model != null && !model.contains("GB") && !model.contains("4G") && !model.contains("3G")
				&& !model.contains("2G");
	}

	private String find(String str, String regex) {
		Pattern pat = Pattern.compile(regex);
		Matcher mat = pat.matcher(str);
		if (mat.find()) {
			return mat.group(1);
		}
		return null;
	}

	public static void bs() {
		bs.append("a").append("a").append("a").append("a").append("a").append("a").append("a").append("a").append("a")
				.append("a");
		String s = bs.toString();
		bs.delete(0, bs.length() - 1);
	}
	
	
	protected static List<String> findByRegexs(String str, String regEx, int group) {
		
		if ((str == null) || (regEx == null) || ((regEx != null) && ("".equals(regEx.trim()))))
			return null;

		Pattern p = Pattern.compile(regEx);
		Matcher m = p.matcher(str);
		ArrayList<String> results = new ArrayList<String>();
		/*while (m.find()) {
			results.add(m.group(group));
		}*/
		
		String result = "";
		Set<String> phoneset = new HashSet<String>(); 
		while (m.find()) {
			phoneset.add( m.group(2));
			result = result + m.group(1) + m.group(2) + ";";
		}
		if(phoneset.size()>=2) {
			System.out.println(result);
		}
		return results;
	}
	
	protected static String findByRegex(String str, String regEx, int group) {
		String resultValue = null;
		if ((str == null) || (regEx == null) || ((regEx != null) && ("".equals(regEx.trim()))))
			return resultValue;

		Pattern p = Pattern.compile(regEx);
		Matcher m = p.matcher(str);

		boolean result = m.find();
		if (result) {
			resultValue = m.group(group);
		}
		return resultValue;
	}
	
	protected static int findPhCount(String str, String regEx, int group) {
		int resultValue = 0;
		if ((str == null) || (regEx == null) || ((regEx != null) && ("".equals(regEx.trim()))))
			return 0;

		Pattern p = Pattern.compile(regEx);
		Matcher m = p.matcher(str);

		while ( m.find()) {
			resultValue++;
		}
		return resultValue;
	}
	
	protected static String[] findAllByRegex(String str, String regEx) {
		if ((str == null) || (regEx == null) || ((regEx != null) && ("".equals(regEx.trim()))))
			return null;

		Pattern p = Pattern.compile(regEx);
		Matcher m = p.matcher(str);
		String[] results = null;
		if (m.find()) {
			results = new String[m.groupCount()];
			for(int i=0; i<results.length; i++){
					results[i] = m.group(i+1);
			}
		}
		return results;
	}
}

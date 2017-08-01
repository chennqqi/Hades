package com.aotain.project.gdtelecom.ua.util;

import java.util.HashMap;
import java.util.Map;

import com.aotain.project.gdtelecom.ua.pojo.Pair;


public class CalTimeUtil {

	private static Map<String, Pair<Long, Long>> map = new HashMap<String,Pair<Long, Long>>();
	
	public static void start(String name) {
		if(!map.containsKey(name)) {
			Pair<Long, Long> pair = new Pair<Long, Long>(System.currentTimeMillis(), 0L);
			map.put(name, pair);
		} else {
			Pair<Long, Long> p = map.get(name);
			p.setKey(System.currentTimeMillis());
		}
	}
	
	public static void end(String name){
		Pair<Long, Long> p = map.get(name);
		long time = System.currentTimeMillis() - p.getKey(); 
		p.setValue(time + p.getValue());
	}
	
	public static void print(){
		for(String name : map.keySet()) {
			System.out.println(name + ":" + map.get(name).getValue());
		}
	}
}

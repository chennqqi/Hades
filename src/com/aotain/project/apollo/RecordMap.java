package com.aotain.project.apollo;

import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;

public class RecordMap {

	HashMap<String, RecordValue> map = new HashMap<String, RecordValue>();
	  
	public void Add(String key, String value) {
		RecordValue record = map.get(key);
	    if (record == null) {
	    	record = new RecordValue();
	      	map.put(key, record);
	    }
	    //count.value += increment;
	}
	  
	  
	public String getValue(String key) {
		RecordValue count = map.get(key);
	    if (count != null) {
	    	return count.value;
	    } else {
	    	return "";
	    }
	}
	  
	public Set<Entry<String, RecordValue>> entrySet() {
	    return map.entrySet();
	}
	  
	public void clear() {
	    map.clear();
	}
	  
	public static class RecordValue {
	    public String value;
	}
}

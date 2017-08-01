package com.aotain.project.ecommerce.rule;

import java.util.ArrayList;
import java.util.List;

import com.aotain.project.ecommerce.pojo.Pair;

public class Rule {
	private String host;
	private List<Pair> pairList = new ArrayList<Pair>();
	
	public Rule() {
		super();
	}
	
	public Rule(String host) {
		super();
		this.host = host;
	}


	public Rule(String host, List<Pair> pairList) {
		super();
		this.host = host;
		this.pairList = pairList;
	}

	public void addPair(Pair pair) {
		if(!pairList.contains(pair)) {
			pairList.add(pair);
		}
	}
	
	public String getPairKFromPairV(String pairValue) {
		for(Pair p : pairList) {
			if(p.getValue().equalsIgnoreCase(pairValue)) {
				return p.getKey();
			}
		}
		return null;
	}
	
	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}


	public List<Pair> getColumnPair() {
		return pairList;
	}


	public void setColumnPair(List<Pair> pairList) {
		this.pairList = pairList;
	}

	
	
}

package com.aotain.project.tm.parse.common.pojo;

public class MatchResult {
	private Device device;
	private MatchEnum matchType;
	
	public MatchResult() {
		super();
	}
	public MatchResult(Device device, MatchEnum matchType) {
		super();
		this.device = device;
		this.matchType = matchType;
	}
	public Device getDevice() {
		return device;
	}
	public void setDevice(Device device) {
		this.device = device;
	}
	public MatchEnum getMatchType() {
		return matchType;
	}
	public void setMatchType(MatchEnum matchType) {
		this.matchType = matchType;
	}
	@Override
	public String toString() {
		return "MatchResult [device=" + device + ", matchType=" + matchType + "]";
	}
	
	
	
}

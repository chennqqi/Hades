package com.aotain.project.ecommerce.pojo;

/**
 * 表to_opr_postorig
 * @author Administrator
 *
 */
public class Post {
	private String username;
	private String prototype;
	private String srcip;
	private String destip;
	private String srcport;
	private String destport;
	private String host;
	private String url;
	private String refer;
	private String useragent;
	private String cookie;
	private String createtime;
	private String postlen;
	private String postcont;
	
	public Post(String arr[]) {
		setUsername(arr[0]);
		setPrototype(arr[1]);
		setSrcip(arr[2]);
		setDestip(arr[3]);
		setSrcport(arr[4]);
		setDestport(arr[5]);
		setHost(arr[6]);
		setUrl(arr[7]);
		setRefer(arr[8]);
		setUseragent(arr[9]);
		setCookie(arr[10]);
		setCreatetime(arr[11]);
		setPostlen(arr[12]);
		setPostcont(arr[13]);
	}
	
	
	public Post() {
		super();
	}


	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPrototype() {
		return prototype;
	}
	public void setPrototype(String prototype) {
		this.prototype = prototype;
	}
	public String getSrcip() {
		return srcip;
	}
	public void setSrcip(String srcip) {
		this.srcip = srcip;
	}
	public String getDestip() {
		return destip;
	}
	public void setDestip(String destip) {
		this.destip = destip;
	}
	public String getSrcport() {
		return srcport;
	}
	public void setSrcport(String srcport) {
		this.srcport = srcport;
	}
	public String getDestport() {
		return destport;
	}
	public void setDestport(String destport) {
		this.destport = destport;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getRefer() {
		return refer;
	}
	public void setRefer(String refer) {
		this.refer = refer;
	}
	public String getUseragent() {
		return useragent;
	}
	public void setUseragent(String useragent) {
		this.useragent = useragent;
	}
	public String getCookie() {
		return cookie;
	}
	public void setCookie(String cookie) {
		this.cookie = cookie;
	}
	public String getCreatetime() {
		return createtime;
	}
	public void setCreatetime(String createtime) {
		this.createtime = createtime;
	}
	public String getPostlen() {
		return postlen;
	}
	public void setPostlen(String postlen) {
		this.postlen = postlen;
	}
	public String getPostcont() {
		return postcont;
	}
	public void setPostcont(String postcont) {
		this.postcont = postcont;
	}
	
	
}


package com.aotain.project.apollo;

import java.io.Serializable;

public class PortInfo implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5222584375472836067L;

	public PortInfo(int p, int s, String d)
	{
		port = p;
		Desc = d;
		score = s;
	}

	public int port;
	
	public int score;
	
	public String Desc;
}

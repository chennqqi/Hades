package dmpcommon.config;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class KvItem implements Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public KvItem()
	{
		
	}
	
	public String ID="";
	public Map<Integer,String> CondMap = new HashMap<Integer,String>();
	public int GetField = -1;
	public int GetIndex = -1;
	public String GetRegx = "";
	public String Code = "";
	public int Weight = 1;
	@Override
	public String toString() {
		return "KvItem [ID=" + ID + ", CondMap.size=" + CondMap.size() + ", GetField=" + GetField + ", GetIndex=" + GetIndex
				+ ", GetRegx=" + GetRegx + ", Code=" + Code + ", Weight=" + Weight + "]";
	}
	
	
}

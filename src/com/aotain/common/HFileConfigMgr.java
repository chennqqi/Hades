package com.aotain.common;

import java.io.File;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;




/**
 * HFile锟斤拷锟斤拷锟斤拷锟�
 * @author Administrator
 *
 */
public class HFileConfigMgr {
	
	/**
	 * 锟斤拷锟斤拷锟侥硷拷路锟斤拷
	 */
	//private static final String configFilePath = System.getProperty("user.dir") + File.separator + "conf" + 
	//    	File.separator ;
	
	public HfileConfig config = new HfileConfig();
	
	public HFileConfigMgr(String XMLFile)
			throws Exception
	{
		init(XMLFile);
	}
	
	private void init(String XMLFile)
			throws Exception
	{
		File f = new File(XMLFile);
		if ((!f.exists()) || (!f.isFile())) {
			System.out.println("XML is not exists");
			System.out.println(XMLFile);
			return;
		}
		
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = null;

		doc = builder.parse(f);

		NodeList pn = doc.getElementsByTagName("HFILE");
		if (pn.getLength() >= 1)
		{
			if ((doc.getElementsByTagName("KVSPLIT") != null) && 
					(doc.getElementsByTagName("KVSPLIT").item(0) != null) && 
					(doc.getElementsByTagName("KVSPLIT").item(0).getFirstChild() != null))
			{
				config.setKVSplit(doc.getElementsByTagName("KVSPLIT").item(0).getFirstChild().getNodeValue());
			}
			
			if ((doc.getElementsByTagName("FIELDSPLIT") != null) && 
					(doc.getElementsByTagName("FIELDSPLIT").item(0) != null) && 
					(doc.getElementsByTagName("FIELDSPLIT").item(0).getFirstChild() != null))
			{
				config.setFieldSplit(doc.getElementsByTagName("FIELDSPLIT").item(0).getFirstChild().getNodeValue());
			}
			
			if ((doc.getElementsByTagName("DTFIELD") != null) && 
					(doc.getElementsByTagName("DTFIELD").item(0) != null) && 
					(doc.getElementsByTagName("DTFIELD").item(0).getFirstChild() != null))
			{
				config.setDtField(doc.getElementsByTagName("DTFIELD").item(0).getFirstChild().getNodeValue());
			}
			
			if ((doc.getElementsByTagName("DTFORMAT") != null) && 
					(doc.getElementsByTagName("DTFORMAT").item(0) != null) && 
					(doc.getElementsByTagName("DTFORMAT").item(0).getFirstChild() != null))
			{
				config.setDtFormat(doc.getElementsByTagName("DTFORMAT").item(0).getFirstChild().getNodeValue());
			}
			
			if ((doc.getElementsByTagName("TABLENAME") != null) && 
					(doc.getElementsByTagName("TABLENAME").item(0) != null) && 
					(doc.getElementsByTagName("TABLENAME").item(0).getFirstChild() != null))
			{
				config.setTableName(doc.getElementsByTagName("TABLENAME").item(0).getFirstChild().getNodeValue());
			}
		}
		
		ArrayList<FieldItem> columns = new ArrayList<FieldItem>();
		if ((doc.getElementsByTagName("COLUMNS").item(0) != null) && 
				(doc.getElementsByTagName("COLUMNS").item(0).getFirstChild() != null))
		{
			Node fieldnode = doc.getElementsByTagName("COLUMNS").item(0);
			parseFieldInfo(columns, fieldnode);
		}
		config.setColumns(columns);
		
		ArrayList<FieldItem> rowkey = new ArrayList<FieldItem>();
		if ((doc.getElementsByTagName("ROWKEY").item(0) != null) && 
				(doc.getElementsByTagName("ROWKEY").item(0).getFirstChild() != null))
		{
			Node fieldnode = doc.getElementsByTagName("ROWKEY").item(0);
			parseFieldInfo(rowkey, fieldnode);
		}
		config.setRowKey(rowkey);
		
		ArrayList<FieldItem> filter = new ArrayList<FieldItem>();
		if ((doc.getElementsByTagName("FILTER").item(0) != null) && 
				(doc.getElementsByTagName("FILTER").item(0).getFirstChild() != null))
		{
			Node fieldnode = doc.getElementsByTagName("FILTER").item(0);
			parseFieldInfo(filter, fieldnode);
		}
		config.setFilter(filter);
	}
	
	private void parseFieldInfo(ArrayList<FieldItem> columns, Node currentNode)
  	{
		NodeList Ssn = currentNode.getChildNodes();

  		for (int nIndex = 0; nIndex < Ssn.getLength(); nIndex++)
  		{
  			Node tempnode = Ssn.item(nIndex);

  			if ((tempnode.getNodeType() != 1) || 
  					(!tempnode.getNodeName().toUpperCase().equals("FIELDITEM")))
  				continue;
  			NodeList childnodeList = tempnode.getChildNodes();
  			if (childnodeList == null)
  				continue;
  			FieldItem field = new FieldItem();
  			for (int i = 0; i < childnodeList.getLength(); i++)
  			{
  				Node childnode = childnodeList.item(i);
  				if (childnode.getNodeType() != 1)
  					continue;
  				String NodeName = childnode.getNodeName().toUpperCase();
  				String strValue = getNodeValue(childnode);
  				if (NodeName.equals("FIELDINDEX"))
  				{
  					field.FieldIndex = Integer.parseInt(strValue);
  				}
  				else if (NodeName.equals("FIELDNAME"))
  				{
  					field.FieldName = strValue;
  				}
  				else if (NodeName.equals("REGEXP"))
  				{
  					field.RegExp = strValue;
  				}
  				else if (NodeName.equals("COLTYPE"))
  				{
  					field.ColType = strValue;
  				}
  				else if (NodeName.equals("PERIOD"))
  				{
  					field.Period = Integer.parseInt(strValue);
  				}
  				else if (NodeName.equals("DECODE"))
  				{
  					field.Decode = strValue;
  				}
  			}
  			columns.add(field);
  		}
  	}
		
	private String getNodeValue(Node CurrentNode)
		{
			String strValue = "";
			NodeList nodelist = CurrentNode.getChildNodes();
			if (nodelist != null)
			{
				for (int i = 0; i < nodelist.getLength(); i++)
				{
					Node tempnode = nodelist.item(i);
					if (tempnode.getNodeType() != 3)
						continue;
					strValue = tempnode.getNodeValue();
				}
			}
			return strValue;
		}
	
	public class FieldItem
	{
		public FieldItem()
		{
			
		}
		public String FieldName = "";
		public int FieldIndex = -1;
		public String RegExp = "";
		public String Decode = "";
		public String ColType = "";
		public int Period = 7;
	}
}

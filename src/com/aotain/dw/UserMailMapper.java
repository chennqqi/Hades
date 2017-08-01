package com.aotain.dw;


import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import java.util.Map.Entry;  
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DefaultStringifier;


public class UserMailMapper extends  TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {

	 @Override
	 public void map(ImmutableBytesWritable row, Result values,Context context) throws IOException 
	 {
		 try
		 {
			 //用户列表传参
			 HashSet<String> userlist = DefaultStringifier.load(context.getConfiguration(), "userlist", HashSet.class);
			 HashMap<String,String> templist = new HashMap<String,String>();
			 HashMap<String,String> maillist = new HashMap<String,String>();
			 int cnt = 0;
			 String mailkey = "";
			 String userkey = "";
			 //用户账号
			 userkey = Bytes.toString(values.getValue(Bytes.toBytes("USERINFO"), Bytes.toBytes("account")));
			 
			 if(userlist.contains(Bytes.toString(values.getRow())))//在用户列表张存在
			 {
				 for (Cell cell : values.listCells()) 
				 {
					 if (Bytes.toString(CellUtil.cloneQualifier(cell)).startsWith("freq_mail"))
					 {
						 templist.put(Bytes.toString(CellUtil.cloneQualifier(cell)).substring(5), 
								Bytes.toString(CellUtil.cloneValue(cell)));//key:'mailn',value:cnt
						 break;
					 }
					 
					 if (Bytes.toString(CellUtil.cloneQualifier(cell)).startsWith("mail")
						&& Bytes.toString(CellUtil.cloneValue(cell)).contains("@"))//合法性过滤
					 {
						maillist.put(Bytes.toString(CellUtil.cloneValue(cell)), //key:mail,value:cnt
								templist.get(Bytes.toString(CellUtil.cloneQualifier(cell))));
					 }
				 }
			 }
			 
			 if(maillist.size()>0)//找到出现频次最多的邮箱
			 {
				 Set<Entry<String, String>> sets = maillist.entrySet();
				 for(Entry<String, String> entry : sets) 
				 {  
					 if(Integer.parseInt(entry.getValue())>cnt)
					 {
						 mailkey = entry.getKey();
					 }
			     }  
				 
				 if(mailkey != "" && userkey != "")
				 {
				     ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes(userkey));
				     ImmutableBytesWritable value = new ImmutableBytesWritable(Bytes.toBytes(mailkey.substring(9)));

				     context.write(key,value);
				 }
			 }
		} 
		catch (InterruptedException e) 
		{
			e.printStackTrace();
		}      
	 }	
}

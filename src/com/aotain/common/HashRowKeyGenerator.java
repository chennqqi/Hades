package com.aotain.common;

import org.apache.hadoop.hbase.util.Bytes;  
import org.apache.hadoop.hbase.util.MD5Hash;  



/** 
* 
* 
**/  
public class HashRowKeyGenerator implements RowKeyGenerator {  
    private static long currentId = 1;  
    private static long currentTime = System.currentTimeMillis();  
    //private static Random random = new Random();  
  
    public byte[] nextId()   
    {  
        try {  
            currentTime = getRowKeyResult(Long.MAX_VALUE - currentTime);  
            byte[] lowT = Bytes.copy(Bytes.toBytes(currentTime), 4, 4);  
            byte[] lowU = Bytes.copy(Bytes.toBytes(currentId), 4, 4);  
            byte[] result = Bytes.add(MD5Hash.getMD5AsHex(Bytes.add(lowT, lowU))  
                    .substring(0, 8).getBytes(), Bytes.toBytes(currentId));  
            return result;  
        } finally {  
            currentId++;  
        }  
    }  
      
    /** 
     *  getRowKeyResult 
     * @param tmpData 
     * @return 
     */  
    public static long getRowKeyResult(long tmpData)  
    {  
        String str = String.valueOf(tmpData);  
        StringBuffer sb = new StringBuffer();  
        char[] charStr = str.toCharArray();  
        for (int i = charStr.length -1 ; i > 0; i--)  
        {  
            sb.append(charStr[i]);  
        }  
          
        return Long.parseLong(sb.toString());  
    }  
    
   
}  
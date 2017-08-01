package com.aotain.project.szreport;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aotain.common.CommonFunction;

public class RadiusDataMapper extends Mapper<LongWritable, Text, Text, Text>{

	protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,Text>.Context context) throws java.io.IOException ,InterruptedException {
		try{
			/**
			 *  0  username (string)  
				1  status (int) 
				2  nasip (bigint)
				3  userip (bigint)
				4  nastag (string)
				5  rxpackets (bigint)
				6  txpackets (bigint)
				7  rxbytes (bigint)
				8  txbytes (bigint)
				9  rxbytes4gflg (bigint)
			   10  txbytes4gflg (bigint)
			   11  sessionid (string)
			   12  sessiontime (bigint)
			   13  eventtime (bigint)
			   14  partdate (string)
			 */
			String[] items = value.toString().split("\\|",-1);
			if(items != null && items.length == 17){
				 //��֤�û��˺ŵĺϷ���
				 if(!validateUser(items[0]))
					 return;
				 
				 String accesstime = CommonFunction.findByRegex(items[13], "[0-9]*", 0);
				 if(accesstime==null)
					 return;
				 
				 if(!validateStatus(items[1])){
					 return;
				 }
				 //�����˺������ڵ�
				 String szCityCodeHeader = context.getConfiguration().get("city.data"); 
				 
				 boolean flag = filterSzUser(items[0], szCityCodeHeader);
				 if(!flag){
					 return;
				 }
				 
				//�����ʽ �� �˺�|״̬|NAS IP|�û�IP|NAS�豸��ʶ|�յ�����|��������|�յ��ֽ���|�����ֽ���|�յ��ֽ�4G����|�����ֽ�4G����|session_id|session_time|event_time
				long rxbytes =  Long.parseLong(items[7])+Long.parseLong(items[9])*1024*1024*1024*4;//�����ֽ���
				long txbytes = Long.parseLong(items[8])+Long.parseLong(items[10])*1024*1024*1024*4;//�����ֽ���
				
				String nasip = int2ip(Long.parseLong(items[2]));
				String userip = int2ip(Long.parseLong(items[3]));
				
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				
				long time = Long.parseLong(items[13])*1000;
				String eventtime = sdf.format(new Date(time));
				
				if(items[0] != null && items[0].trim().length() > 0){
					String natip = int2ip(Long.parseLong(items[14]));
					String content = items[0]+","+items[1]+","+nasip+","+items[4]+","+userip+","+items[5]+","+items[6]+","+rxbytes
							+","+txbytes+","+items[12]+","+eventtime+","+natip+","+items[15]+","+items[16];
					context.write(new Text(content), new Text(""));
				}
			}
		}catch (Exception e) {
		
		}
	};
	
	 /**
		 * ����ת��ΪIP
		 * @param ipInt
		 * @return
		 */
		private static String int2ip(long ipInt){
			
			StringBuilder sb=new StringBuilder();
			sb.append((ipInt>>24)&0xFF).append(".");
			sb.append((ipInt>>16)&0xFF).append(".");
			sb.append((ipInt>>8)&0xFF).append(".");
			sb.append(ipInt&0xFF);
			return sb.toString(); 
			
		} 
		
		private static boolean validateStatus(String status){
			String[] str ={"1","2","3"};
			for (String s : str) {
				if(s.equals(status)){
					return true;
				}
			}
			return false;
		}
		
		private boolean validateUser(String username)
		{
			boolean ret = false;
			
			String aa = CommonFunction.findByRegex(username, "^([a-zA-Z0-9_\\-\\.]{1,15})@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.)|(([a-zA-Z0-9\\-]+\\.)+))(com|cn|net|com.cn|org|gd)(\\]?)$", 0);
			if(aa == null)
			{
				aa = CommonFunction.findByRegex(username, "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}", 0);
				if(aa == null)
					ret = false;
				else
					ret = true;
			}
			else
			{
				ret = true;
			}
			
			return ret;
		}
		
		
		private boolean filterSzUser(String username,String szCityCodeHeader){
			if(szCityCodeHeader.endsWith("|")){
				szCityCodeHeader = szCityCodeHeader.substring(0, szCityCodeHeader.length()-1);
			}
			boolean ret = false;
			String[] cityArr = szCityCodeHeader.split("\\|",-1);
			for (String str : cityArr) {
				ret = username.startsWith(str);
				if(ret){
					break;
				}
			}
			return ret;
		}
		
}

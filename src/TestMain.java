import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;

import com.aotain.common.CommonFunction;
import com.aotain.project.apollo.ApolloConfig;
import com.aotain.project.apollo.IPDatabase;




public class TestMain {
	
	
	public static void main(String[] args) throws Exception {
		


	}
	
	public static  String findByRegex(String str, String regEx, int group)
 	{
 		String resultValue = null;
 		if ((str == null) || (regEx == null) || ((regEx != null) && ("".equals(regEx.trim())))) 
 			return resultValue;
 		
 		
 		Pattern p = Pattern.compile(regEx);
 		Matcher m = p.matcher(str);

 		boolean result = m.matches(); 
 		if (result)
 		{
 			resultValue = m.group(group);
 		}
 		return resultValue;
 	}
	
	private static boolean validateUser(String username)
	{
		boolean ret = false;
		
		String aa = CommonFunction.findByRegex(username, "^([a-zA-Z0-9_\\-\\.]{1,50})@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.)|(([a-zA-Z0-9\\-]+\\.)+))(com|cn|net|com.cn|org|gd)(\\]?)$", 0);
		if(aa == null)
		{
			aa = CommonFunction.findByRegex(username, "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}", 0);
			if(aa == null)
			{
				if(username.indexOf("IPCYW")==0)
					ret = true;
			}
			else
			{
				ret = true;
			}
		}
		else
		{
			ret = true;
		}
		
		return ret;
	}
}

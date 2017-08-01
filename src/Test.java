import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.aotain.common.CommonFunction;


public class Test {

			public static void main(String[] args) throws IOException {
				//getKeywordConfig("C:/keyword");
				// getDomain();
				  String ip = InetAddress.getLocalHost().getHostAddress();
			        System.out.println(ip);
				
				
			}

			private  static void getKeywordConfig(String filePath) {
				   String column = "";
					File file=new File(filePath);
					try {
						 InputStreamReader read = new InputStreamReader(new FileInputStream(file));
		                 BufferedReader bufferedReader = new BufferedReader(read);
		                 String lineTxt = null;
		                 while((lineTxt = bufferedReader.readLine()) != null){
		    				column += lineTxt.trim() +"#";  
		                 }
		                 column  = column.substring(0,column.length() -1 );
	                  	System.out.println(column);
					} catch (Exception e) {
						// TODO: handle exception
					}
					  
			}
			public static void getDomain(){
				String s="http://news.baidu.com/";
				String domain=CommonFunction.getDomain(s);
				System.out.println(domain);
			}
}

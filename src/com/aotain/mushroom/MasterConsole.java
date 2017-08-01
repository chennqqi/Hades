package com.aotain.mushroom;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import net.sf.json.JSONObject;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.log4j.Logger;


public class MasterConsole {
	
	private Logger log = Logger.getRootLogger();
	
	ServerSocket serverSocket;
	
	
	private Thread mainThread;
	
	public MasterConsole(int p)
	{
		try 
		{
			//建立Socket监听
			serverSocket=new ServerSocket(p);
			log.debug("Start Master Socket Port[" + p + "] listener Success!");
		} catch (IOException e) {
			log.warn("Start Master Socket Port[" + p + "] listener Failure!",e);
		}
	}
	
	/**
	 * 启动服务
	 */
 	public void start()
 		throws IOException
    {
 		this.mainThread = new Thread(new Runnable()
 		{
 			public void run()
 			{
 				service("master start");
 			}
 		});
 		this.mainThread.start();
    }
 	
 	/**
 	 * 监听服务
 	 * @param msg
 	 */
 	public void service(String msg)
	{
		while(true)
		{
			Socket socket=null;
			try {
				
				socket=serverSocket.accept();
				Thread workThread=new Thread(new Handler(socket,msg));
				workThread.start();
				//Thread.sleep(1000L);
			} catch (Exception e) {
				// TODO 自动生成 catch 块
				//e.printStackTrace();
				log.error("Master console service error",e);
			} finally
			{
				
			}
		}
	}
}

class Handler implements Runnable{   //负责与单个客户的通信
	
	private Logger log = Logger.getRootLogger();
	private Socket socket;
	private String infomation;
	  
	public Handler(Socket socket,String infomation){
		this.socket=socket;
		this.infomation=infomation;
	}
	private PrintWriter getWriter(Socket socket)throws IOException{
		return new PrintWriter(socket.getOutputStream());
	}
	private BufferedReader getReader(Socket socket)throws IOException{
		return new BufferedReader(new InputStreamReader(socket.getInputStream()));
	}
	  
	public void run()
	{
		PrintWriter pw = null;
		try 
		{
			log.debug(this.infomation + "--New connection accepted " +
					socket.getInetAddress() + ":" +socket.getPort());
			BufferedReader br = getReader(socket);
			pw = getWriter(socket);
			
			
			String line = br.readLine(); //收到客户端信息
			String msg = "";
			while(line!=null&&!line.equals("bye"))
			{
				msg = msg + line;
				line=br.readLine();
			}
		      
			log.debug("客户端输出信息："+msg);
			pw.println("Done");//返回客户端信息
			pw.flush();
		      //socket.close();
		      
			if(!msg.isEmpty())
			{
				//解析返回信息
				JSONObject jsonObject = JSONObject.fromObject(msg); 
				Object bean = JSONObject.toBean(jsonObject);
				int MsgID = 0;
				try 
				{
					assertEquals(jsonObject.get("msgID"),
							PropertyUtils.getProperty(bean, "msgID"));
					//获取到消息号
					MsgID = Integer.parseInt(PropertyUtils.getProperty(bean, "msgID").toString());
		    		  
				} catch (Exception e) {
					// TODO Auto-generated catch block
					log.error("Master,解析客户端JSON MSGID发生异常",e);
				}
				
				//服务器端消息列表
				switch(MsgID)
				{
					case 1001://HBASE 入库日志
						HBaseImportLog obj = new HBaseImportLog();
						MushroomWarehouse.getInstance().InsertHBaseImportLog(obj.getByJson(msg));
						break;
					default:
						break;	  
				 }
					
		     }
		      
		}catch (IOException e) {
			  log.error("Master,接受消息异常",e);
			  pw.println(e.getMessage());//返回客户端信息
		}finally {
			try{
				  if(pw!=null)
					  pw.close();
				  if(socket!=null)
					  socket.close(); //断开连接
				  
			  }catch (IOException e) 
			  {
				  log.error("Master,close socket 异常",e);
			  }
		  }
	  }
}

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
			//����Socket����
			serverSocket=new ServerSocket(p);
			log.debug("Start Master Socket Port[" + p + "] listener Success!");
		} catch (IOException e) {
			log.warn("Start Master Socket Port[" + p + "] listener Failure!",e);
		}
	}
	
	/**
	 * ��������
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
 	 * ��������
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
				// TODO �Զ����� catch ��
				//e.printStackTrace();
				log.error("Master console service error",e);
			} finally
			{
				
			}
		}
	}
}

class Handler implements Runnable{   //�����뵥���ͻ���ͨ��
	
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
			
			
			String line = br.readLine(); //�յ��ͻ�����Ϣ
			String msg = "";
			while(line!=null&&!line.equals("bye"))
			{
				msg = msg + line;
				line=br.readLine();
			}
		      
			log.debug("�ͻ��������Ϣ��"+msg);
			pw.println("Done");//���ؿͻ�����Ϣ
			pw.flush();
		      //socket.close();
		      
			if(!msg.isEmpty())
			{
				//����������Ϣ
				JSONObject jsonObject = JSONObject.fromObject(msg); 
				Object bean = JSONObject.toBean(jsonObject);
				int MsgID = 0;
				try 
				{
					assertEquals(jsonObject.get("msgID"),
							PropertyUtils.getProperty(bean, "msgID"));
					//��ȡ����Ϣ��
					MsgID = Integer.parseInt(PropertyUtils.getProperty(bean, "msgID").toString());
		    		  
				} catch (Exception e) {
					// TODO Auto-generated catch block
					log.error("Master,�����ͻ���JSON MSGID�����쳣",e);
				}
				
				//����������Ϣ�б�
				switch(MsgID)
				{
					case 1001://HBASE �����־
						HBaseImportLog obj = new HBaseImportLog();
						MushroomWarehouse.getInstance().InsertHBaseImportLog(obj.getByJson(msg));
						break;
					default:
						break;	  
				 }
					
		     }
		      
		}catch (IOException e) {
			  log.error("Master,������Ϣ�쳣",e);
			  pw.println(e.getMessage());//���ؿͻ�����Ϣ
		}finally {
			try{
				  if(pw!=null)
					  pw.close();
				  if(socket!=null)
					  socket.close(); //�Ͽ�����
				  
			  }catch (IOException e) 
			  {
				  log.error("Master,close socket �쳣",e);
			  }
		  }
	  }
}

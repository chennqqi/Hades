package com.aotain.mushroom;

import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * Kafka Éú²úÕß
 * @author Administrator
 *
 */
public class MushroomProducer extends Thread {
	
	 private kafka.javaapi.producer.Producer<Integer, String> producer;  
	 private String topic;  
	 private Properties props = new Properties();  
	 
	 public MushroomProducer(String topic)  
	 {  
		 props.put("serializer.class", "kafka.serializer.StringEncoder");  
		 props.put("metadata.broker.list", "10.22.10.139:9092");  
		 producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));  
		 this.topic = topic;  
	 }  
	  
	    
	 @Override  
	 public void run() {  
		 int messageNo = 1;  
		 while (true)  
		 {  
			 String messageStr = new String("Message_" + messageNo);  
			 System.out.println("Send:" + messageStr);  
			 producer.send(new KeyedMessage<Integer, String>(topic, messageStr));  
			 messageNo++;  
			 try {  
				 sleep(3000);  
			 } catch (InterruptedException e) {  
              // TODO Auto-generated catch block  
				 e.printStackTrace();  
			 }  
		 }  
	 }  
}

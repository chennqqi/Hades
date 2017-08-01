package com.aotain.datamining;

import java.util.ArrayList;
import java.util.HashMap;

public class KMeansClusterThread extends Thread{
	
	private HashMap<Integer,float[]> _data;
	private HashMap<Integer,Integer> _noises;
	public KMeansClusterThread(HashMap<Integer,float[]> data,
			HashMap<Integer,Integer> noises)
	{
		this._data = data;
	}
	
	public ArrayList<Integer> initials = new ArrayList<Integer>();
	
	public void run(){
	   
	       synchronized(this){
	           try{
	              
	        	 //a,b为标志距离最远的两个向量的索引
	               int j,a,b;
	               a = b = 0;
	              
	               int count = 0;
	               //最远距离
	               float maxDis = 0;
	              
	               //已经找到的初始点个数
	               int alreadyCls = 2;
	              
	               //存放已经标记为初始点的向量索引
	               

	               //从两个开始
	               for(int keyi : _data.keySet())
	               {
	            	   //噪声点
	            	   if(_noises.containsKey(keyi))
	            		   continue;
	            	   //long startTime = System.currentTimeMillis();
	            	   //j = i + 1;
	            	   for(int keyj : _data.keySet())
	            	   {
	            		   if(keyi == keyj)
	            			   continue;
	            		   //噪声点
	            		   if(_noises.containsKey(keyj))
	            			   continue;
	            		   //找出最大的距离并记录下来
	            		   float newDis = calDis(_data.get(keyi),_data.get(keyj));
	            		   if(maxDis < newDis)
	            		   {
	            			   a = keyi;
	            			   b = keyj;
	            			   maxDis = newDis;
	            		   }
	            	   }
	            	   
	            	   count++;
	            	   if(keyi%1000==0)
	            	   {
	            		   System.out.println(this.getId() + " Find Initials Iteration " + count + "/" + _data.size());
	            	   }
	            	   //long endTime = System.currentTimeMillis();
	            	   //System.out.println(i + "Vector Caculation Time:"+(endTime-startTime)+"ms");
	               }
	              
	               //将前两个初始点记录下来
	               initials.add(a);
	               initials.add(b);
	               
	               //结束通知主线程
	               String strNotify = String.format("Thread-%s OK", this.getId());
	               System.out.println(strNotify);
	               this.notify();
	        	   
	           }catch(Exception e){
	              e.printStackTrace();
	           }
	       }
	    }
	
    /**
     * 计算a样本和b样本的欧式距离作为不相似度
     * 
     * @param a     样本a
     * @param b     样本b
     * @return      欧式距离长度
     */ 
	private float calDis(float[] aVector,float[] bVector)  {
		double dis = 0;
		int i = 0;//0位存放的是ID
         /*最后一个数据在训练集中为结果，所以不考虑  */
          for(;i < aVector.length;i++)
               dis += Math.pow(bVector[i] - aVector[i],2); 
          dis = Math.pow(dis, 0.5); 
          return (float)dis; 
	}
}

package com.aotain.datamining;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.util.ToolRunner;
 
//K-means算法实现
 
public class KMeans {
    //聚类的数目
    final static int ClassCount = 10;
    //样本数目（测试集）
    static int InstanceNumber = 1000000; 
    //样本属性数目（测试）
    static int FieldCount = 0;
    
    //设置异常点阈值参数（每一类初始的最小数目为InstanceNumber/ClassCount^t）
    public double t = 3.0;
    //存放数据的矩阵
    private float[][] data;
    
    private HashMap<Integer,String> dataID = new HashMap<Integer, String>();
   
    //每个类的均值中心
    private float[][] classData;
   
    //噪声集合索引
    private HashMap<Integer,Integer> noises;
   
    //存放每次变换结果的矩阵
    private ArrayList<HashMap<Integer,Integer>> result;

    //构造函数，初始化
    public KMeans(int fieldcount)
    {
    	//最后一位用来储存结果
    	FieldCount = fieldcount;
    	data = new float[InstanceNumber][fieldcount+1];
    	//data = new HashMap<String,float[]>();//[InstanceNumber][FieldCount+2];
    	classData = new float[ClassCount][fieldcount];
    	result = new ArrayList<HashMap<Integer,Integer>>(ClassCount);
    	noises = new HashMap<Integer,Integer>();
  
    }
   
 
   /**
    * 主函数入口
    * 测试集的文件名称为“测试集.data”,其中有1000*57大小的数据
    * 每一行为一个样本，有57个属性
    * 主要分为两个步骤
    * 1.读取数据
    * 2.进行聚类
    * 最后统计运行时间和消耗的内存
    * @param args
    */
   public static void main(String[] args) {
      // TODO Auto-generated method stub
       long startTime = System.currentTimeMillis();
       if(args.length != 4)
       {
    	   System.err.printf("Usage: %s <SourceFile><TargetFile><T><FieldCount>","KMeans");
           ToolRunner.printGenericCommandUsage(System.err);
           System.exit(-1);        
       }
       KMeans cluster = new KMeans(Integer.parseInt(args[3]));
       
       if(args.length >= 3)
    	   cluster.t = Double.parseDouble(args[2]);
       //if(args.length >= 4)
       //   cluster.FieldCount = Integer.parseInt(args[3]);
       
       //读取数据
       cluster.readData(args[0]);
       //聚类过程
       cluster.cluster();
       
       
       //输出结果
       cluster.printResult(args[1]);
       long endTime = System.currentTimeMillis();
       System.out.println("Total Time:"+ (endTime - startTime)/1000+"s");
       System.out.println("Memory Consuming:"+(float)(Runtime.getRuntime().totalMemory() -
          Runtime.getRuntime().freeMemory())/1000000 + "MB");
   }
        /*
         * 读取测试集的数据
         *
         * @param trainingFileName 测试集文件名
         */
   public void readData(String trainingFileName)
   {
       try
       {
    	   File f = new File(trainingFileName);
    	   int line = 0;
    	   
    	   if(f.isDirectory())
    	   {
    		   File files[] = f.listFiles();
    		   
    		   for(File file : files)
    		   {
    			   FileReader fr = new FileReader(file);
    			   BufferedReader br = new BufferedReader(fr);
    	    	   //存放数据的临时变量
    	    	   String lineData = null;
    	    	   String[] splitData = null;
    	      
    	    	   boolean flag = false;
    	    	   //按行读取
    	    	   while(br.ready())
    	    	   {
    	    		   //得到原始的字符串
    	    		   flag = false;
    	    		   lineData = br.readLine();
    	    		   splitData = lineData.split(",");
    	    		   //转化为数据
    	    		   //        	System.out.println("length:"+splitData.length);
    	    		   
    	    		   if(splitData.length>1)
    	    		   {
    	    			   
    	    			   String key = splitData[0];
    	    			   //data[line][0] = line; //第一位用来存放ID
    	    			   for(int i = 1;i < splitData.length;i++)
    	    			   {
    	    				   //              System.out.println(splitData[i]);
    	    				   //              System.out.println(splitData[i].getClass());
    	    				   try
    	    			       {
    	    					   data[line][i-1] = Float.parseFloat(splitData[i]);
    	    			       }
    	    				   catch(Exception e)
    	    				   {//遇到异常属性排除记录
    	    					   data[line][i-1] = 0;
    	    					   System.out.println("foramt error,set 0");
    	    					   flag = true;
    	    					   break;
    	    				   }
    	    			   }
    	    			   
    	    			   if(flag)
    	    				   continue;
    	    			   //行号索引key
    	    			   dataID.put(line, key);
    	    			   //if(line > 20000)
    	    			   //   break;
    	    			   line++;
    	    		   }
    	    	   }
    		   }
    	   }

    	  
    	   System.out.println(dataID.size());
    	   InstanceNumber = dataID.size();
       }catch(Exception e)
       {
    	   e.printStackTrace();
       }
   }

   /*
    * 聚类过程，主要分为两步
    * 1.循环找初始点
    * 2.不断调整直到分类不再发生变化
    */
   public void cluster()
   {
       //数据归一化
       normalize();
       //标记是否需要重新找初始点
       boolean needUpdataInitials = true;
      
       //找初始点的迭代次数
       int times = 1;
       //找初始点
       while(needUpdataInitials)
       {
    	   needUpdataInitials = false;
    	   result.clear();
    	   System.out.println("Find Initials Iteration"+(times++)+"time(s)");
     
    	   //一次找初始点的尝试和根据初始点的分类
    	   findInitials();
    	   firstClassify();
     
    	   //如果某个分类的数目小于特定的阈值，则认为这个分类中的所有样本都是噪声点
    	   //需要重新找初始点
    	   for(int i = 0;i < result.size();i++)
    	   {
    		   if(result.get(i).size() < InstanceNumber/Math.pow(ClassCount,t))
    		   {
    			   needUpdataInitials = true;
    			   noises.putAll(result.get(i));
    			   System.out.println(String.format("reslut[size:%d],%f",result.get(i).size(),(float)InstanceNumber/Math.pow(ClassCount,t)));
    		   }
    	   }
       }
      
       //找到合适的初始点后
       //不断的调整均值中心和分类，直到不再发生任何变化
       Adjust();
   }
  
   /*
    * 对数据进行归一化
    * 1.找每一个属性的最大值
    * 2.对某个样本的每个属性除以其最大值
    */
   public void normalize()
   {
       //找最大值
       float[] max = new float[FieldCount];
       
       for(int i = 0;i < InstanceNumber;i++)
       {
    	   for(int j = 0;j < FieldCount;j++)
    	   {
    		   if(data[i][j] > max[j])
    			   max[j] = data[i][j];
    	   }
       }
      
       //归一化
       for(int i = 0;i < InstanceNumber;i++)
       {
    	   for(int j = 0;j < FieldCount;j++)
    	   {
    		   //BigDecimal bd = new BigDecimal(data[i][j]/max[j]);  
    		   //BigDecimal bd2 = bd.setScale(8,BigDecimal.ROUND_HALF_UP);  
    		  // get_double = Double.ParseDouble(bd2.toString());  
    		   data[i][j] = data[i][j]/max[j];//Float.parseFloat(bd2.toString());  
    	   }
       }
   }
  
   //关于初始向量的一次找寻尝试
   public void findInitials()
   {
	   //a,b为标志距离最远的两个向量的索引
       int i,j,a,b;
       i = j = a = b = 0;
      
       //最远距离
       float maxDis = 0;
      
       //已经找到的初始点个数
       int alreadyCls = 2;
      
       //存放已经标记为初始点的向量索引
       ArrayList<Integer> initials = new ArrayList<Integer>();

       //从两个开始
       for(;i < InstanceNumber;i++)
       {
    	   //噪声点
    	   if(noises.containsKey(i))
    		   continue;
    	   //long startTime = System.currentTimeMillis();
    	   j = i + 1;
    	   for(;j < InstanceNumber;j++)
    	   {
    		   //噪声点
    		   if(noises.containsKey(j))
    			   continue;
    		   //找出最大的距离并记录下来
    		   float newDis = calDis(data[i],data[j]);
    		   if(maxDis < newDis)
    		   {
    			   a = i;
    			   b = j;
    			   maxDis = newDis;
    		   }
    	   }
    	   
    	   if(i%1000==0)
    	   {
    		   System.out.println("Find Initials Iteration " + i + "/" + InstanceNumber);
    	   }
    	   //long endTime = System.currentTimeMillis();
    	   //System.out.println(i + "Vector Caculation Time:"+(endTime-startTime)+"ms");
       }
      
       //将前两个初始点记录下来
       initials.add(a);
       initials.add(b);
       classData[0] = data[a];
       classData[1] = data[b];
      
       //在结果中新建存放某样本索引的对象，并把初始点添加进去
       HashMap<Integer,Integer> resultOne = new HashMap<Integer,Integer>();
       HashMap<Integer,Integer> resultTwo = new HashMap<Integer,Integer>();
       resultOne.put(a,a);
       resultTwo.put(b,b);
       result.add(resultOne);
       result.add(resultTwo);
      
       //找到剩余的几个初始点
       while(alreadyCls < ClassCount)
       {
	      i = j = 0;
	      float maxMin = 0;
	      int newClass = -1;
	     
	      //找最小值中的最大值
	      for(;i < InstanceNumber;i++)
	      {
	          float min = 0;
	          float newMin = 0;
	          //找和已有类的最小值
	          if(initials.contains(i))
	        	  continue;
	          //噪声点去除
	          if(noises.containsKey(i))
	        	  continue;
	          for(j = 0;j < alreadyCls;j++)
	          {
	        	  newMin = calDis(data[i],classData[j]);
	        	  if(min == 0 || newMin < min)
	        		  min = newMin;
	          }
         
	          //新最小距离较大
	          if(min > maxMin)
	          {
	        	  maxMin = min;
	        	  newClass = i;
	          }
	      }
	      //添加到均值集合和结果集合中
	      //System.out.println("NewClass"+newClass);
	      initials.add(newClass);
	      classData[alreadyCls++] = data[newClass];
	      HashMap<Integer,Integer> rslt = new HashMap<Integer,Integer>();
	      rslt.put(newClass,newClass);
	      result.add(rslt);
       }
   }
   
   /**
    * 指定初始化中心点
    */
   public void setInitials()
   {
	   
   }
  
   //第一次分类
   public void firstClassify()
   {
       //根据初始向量分类
       for(int i = 0;i < InstanceNumber;i++)
       {
    	   float min = 0f;
    	   int clsId = -1;
    	   for(int j = 0;j < classData.length;j++)
    	   {
    		   //欧式距离
    		   float newMin = calDis(classData[j],data[i]);
    		   if(clsId == -1 || newMin <min)
    		   {
    			   clsId = j;
    			   min = newMin;
    		   }
         
    	   }
    	   //本身不再添加
    	   if(!result.get(clsId).containsKey(i))
    		   result.get(clsId).put(i,i);
       	}
   	}
   	//迭代分类，直到各个类的数据不再变化
   	public void Adjust()
   	{
       //记录是否发生变化
       boolean change = true;
      
       //循环的次数
       int times = 1;
       while(change)
       {
    	   //复位
    	   change = false;
    	   System.out.println("Adjust Iteration"+(times++)+"time(s)");
                   
    	   //重新计算每个类的均值 
    	   for(int i = 0;i < ClassCount; i++){ 
    		   //原有的数据 
    		   HashMap<Integer,Integer> cls = result.get(i); 
       
    		   //新的均值 
    		   float[] newMean = new float[FieldCount ]; 
       
    		   //计算均值 
    		   for(Integer index:cls.keySet()){ 
    			   for(int j = 0;j < FieldCount ;j++) 
    				   newMean[j] += data[index][j]; 
    		   } 
    		   for(int j = 0;j < FieldCount ;j++) 
    			   newMean[j] /= cls.size(); 
    		   if(!compareMean(newMean, classData[i])){ 
    			   classData[i] = newMean; 
    			   change = true; 
    		   } 
    	   } 
    	   //清空之前的数据 
    	   for(HashMap<Integer,Integer> cls:result) 
    		   cls.clear(); 
        
    	   //重新分配 
    	   for(int i = 0;i < InstanceNumber;i++) 
    	   { 
    		   float min = 0f; 
    		   int clsId = -1; 
    		   for(int j = 0;j < classData.length;j++){ 
    			   float newMin = calDis(classData[j], data[i]); 
    			   if(clsId == -1 || newMin < min){ 
    				   clsId = j; 
    				   min = newMin; 
    			   } 
               } 
               data[i][FieldCount] = clsId; 
                   result.get(clsId).put(i,i); 
           } 
                 
         //测试聚类效果(训练集) 
      //          for(int i = 0;i < ClassCount;i++){ 
      //              int positives = 0; 
      //              int negatives = 0; 
      //              ArrayList<Integer> cls = result.get(i); 
      //              for(Integer instance:cls) 
      //                  if (data[instance][FieldCount - 1] == 1f) 
      //                      positives ++; 
      //                  else 
      //                      negatives ++; 
      //              System.out.println(" " + i + " Positive: " + positives + " Negatives: " + negatives); 
      //          } 
      //          System.out.println(); 
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
         
        /**
         * 判断两个均值向量是否相等
         * 
         * @param a 向量a
              * @param b 向量b
         * @return
         */ 
       private boolean compareMean(float[] a,float[] b) 
       { 
             if(a.length != b.length) 
               return false; 
             for(int i =0;i < a.length;i++){ 
             if(a[i] > 0 &&b[i] > 0&& a[i] != b[i]){ 
                  return false; 
                }    
            } 
              return true; 
        } 
          
        /**
         * 将结果输出到一个文件中
         * 
              * @param fileName
              */ 
         public void printResult(String fileName) 
         { 
        	 FileWriter fw = null; 
        	 BufferedWriter bw = null; 
        	 try { 
                  fw = new FileWriter(fileName); 
                  bw = new BufferedWriter(fw); 
                  //写入文件 
                  for(int i = 0;i < InstanceNumber;i++) 
                  { 
                	  bw.write(dataID.get(i)+ ",") ;
                	  for(int j = 0;j < data[i].length ;j++)
                	  {
                		  bw.write(String.format("%.8f,",data[i][j]));
                	  }
                	  //bw.write(dataID.get((int)data[i][0])+ "," +String.valueOf(data[i][FieldCount]).substring(0, 1)); 
                	  bw.newLine(); 
                  } 
               
               //统计每类的数目，打印到控制台 
               for(int i = 0;i < ClassCount;i++) 
               { 
            	     
            	   
            	     String str = "";
                     
                     float cls[] = classData[i];
                     for(int n = 0;n < FieldCount; n++)
                     {
                    	 str = str + "cls[" + n + "]:" + String.format("%.8f,",cls[n]) + ",";
                     }
                     String output = String.format("No.%d [%d]:%s", (i),result.get(i).size(),str);
                    	 
                     System.out.println(output);
               } 
               
               
        	 } catch (IOException e) { 
             e.printStackTrace(); 
           } finally{ 
                 
               //关闭资源 
             if(bw != null) 
                   try { 
                     bw.close(); 
                   } catch (IOException e) { 
                       e.printStackTrace(); 
                  } 
               if(fw != null) 
                    try { 
                        fw.close(); 
                   } catch (IOException e) { 
                         e.printStackTrace(); 
                    } 
             } 
             
        } 
      } 

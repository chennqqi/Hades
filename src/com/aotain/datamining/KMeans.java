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
 
//K-means�㷨ʵ��
 
public class KMeans {
    //�������Ŀ
    final static int ClassCount = 10;
    //������Ŀ�����Լ���
    static int InstanceNumber = 1000000; 
    //����������Ŀ�����ԣ�
    static int FieldCount = 0;
    
    //�����쳣����ֵ������ÿһ���ʼ����С��ĿΪInstanceNumber/ClassCount^t��
    public double t = 3.0;
    //������ݵľ���
    private float[][] data;
    
    private HashMap<Integer,String> dataID = new HashMap<Integer, String>();
   
    //ÿ����ľ�ֵ����
    private float[][] classData;
   
    //������������
    private HashMap<Integer,Integer> noises;
   
    //���ÿ�α任����ľ���
    private ArrayList<HashMap<Integer,Integer>> result;

    //���캯������ʼ��
    public KMeans(int fieldcount)
    {
    	//���һλ����������
    	FieldCount = fieldcount;
    	data = new float[InstanceNumber][fieldcount+1];
    	//data = new HashMap<String,float[]>();//[InstanceNumber][FieldCount+2];
    	classData = new float[ClassCount][fieldcount];
    	result = new ArrayList<HashMap<Integer,Integer>>(ClassCount);
    	noises = new HashMap<Integer,Integer>();
  
    }
   
 
   /**
    * ���������
    * ���Լ����ļ�����Ϊ�����Լ�.data��,������1000*57��С������
    * ÿһ��Ϊһ����������57������
    * ��Ҫ��Ϊ��������
    * 1.��ȡ����
    * 2.���о���
    * ���ͳ������ʱ������ĵ��ڴ�
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
       
       //��ȡ����
       cluster.readData(args[0]);
       //�������
       cluster.cluster();
       
       
       //������
       cluster.printResult(args[1]);
       long endTime = System.currentTimeMillis();
       System.out.println("Total Time:"+ (endTime - startTime)/1000+"s");
       System.out.println("Memory Consuming:"+(float)(Runtime.getRuntime().totalMemory() -
          Runtime.getRuntime().freeMemory())/1000000 + "MB");
   }
        /*
         * ��ȡ���Լ�������
         *
         * @param trainingFileName ���Լ��ļ���
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
    	    	   //������ݵ���ʱ����
    	    	   String lineData = null;
    	    	   String[] splitData = null;
    	      
    	    	   boolean flag = false;
    	    	   //���ж�ȡ
    	    	   while(br.ready())
    	    	   {
    	    		   //�õ�ԭʼ���ַ���
    	    		   flag = false;
    	    		   lineData = br.readLine();
    	    		   splitData = lineData.split(",");
    	    		   //ת��Ϊ����
    	    		   //        	System.out.println("length:"+splitData.length);
    	    		   
    	    		   if(splitData.length>1)
    	    		   {
    	    			   
    	    			   String key = splitData[0];
    	    			   //data[line][0] = line; //��һλ�������ID
    	    			   for(int i = 1;i < splitData.length;i++)
    	    			   {
    	    				   //              System.out.println(splitData[i]);
    	    				   //              System.out.println(splitData[i].getClass());
    	    				   try
    	    			       {
    	    					   data[line][i-1] = Float.parseFloat(splitData[i]);
    	    			       }
    	    				   catch(Exception e)
    	    				   {//�����쳣�����ų���¼
    	    					   data[line][i-1] = 0;
    	    					   System.out.println("foramt error,set 0");
    	    					   flag = true;
    	    					   break;
    	    				   }
    	    			   }
    	    			   
    	    			   if(flag)
    	    				   continue;
    	    			   //�к�����key
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
    * ������̣���Ҫ��Ϊ����
    * 1.ѭ���ҳ�ʼ��
    * 2.���ϵ���ֱ�����಻�ٷ����仯
    */
   public void cluster()
   {
       //���ݹ�һ��
       normalize();
       //����Ƿ���Ҫ�����ҳ�ʼ��
       boolean needUpdataInitials = true;
      
       //�ҳ�ʼ��ĵ�������
       int times = 1;
       //�ҳ�ʼ��
       while(needUpdataInitials)
       {
    	   needUpdataInitials = false;
    	   result.clear();
    	   System.out.println("Find Initials Iteration"+(times++)+"time(s)");
     
    	   //һ���ҳ�ʼ��ĳ��Ժ͸��ݳ�ʼ��ķ���
    	   findInitials();
    	   firstClassify();
     
    	   //���ĳ���������ĿС���ض�����ֵ������Ϊ��������е�������������������
    	   //��Ҫ�����ҳ�ʼ��
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
      
       //�ҵ����ʵĳ�ʼ���
       //���ϵĵ�����ֵ���ĺͷ��ֱ࣬�����ٷ����κα仯
       Adjust();
   }
  
   /*
    * �����ݽ��й�һ��
    * 1.��ÿһ�����Ե����ֵ
    * 2.��ĳ��������ÿ�����Գ��������ֵ
    */
   public void normalize()
   {
       //�����ֵ
       float[] max = new float[FieldCount];
       
       for(int i = 0;i < InstanceNumber;i++)
       {
    	   for(int j = 0;j < FieldCount;j++)
    	   {
    		   if(data[i][j] > max[j])
    			   max[j] = data[i][j];
    	   }
       }
      
       //��һ��
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
  
   //���ڳ�ʼ������һ����Ѱ����
   public void findInitials()
   {
	   //a,bΪ��־������Զ����������������
       int i,j,a,b;
       i = j = a = b = 0;
      
       //��Զ����
       float maxDis = 0;
      
       //�Ѿ��ҵ��ĳ�ʼ�����
       int alreadyCls = 2;
      
       //����Ѿ����Ϊ��ʼ�����������
       ArrayList<Integer> initials = new ArrayList<Integer>();

       //��������ʼ
       for(;i < InstanceNumber;i++)
       {
    	   //������
    	   if(noises.containsKey(i))
    		   continue;
    	   //long startTime = System.currentTimeMillis();
    	   j = i + 1;
    	   for(;j < InstanceNumber;j++)
    	   {
    		   //������
    		   if(noises.containsKey(j))
    			   continue;
    		   //�ҳ����ľ��벢��¼����
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
      
       //��ǰ������ʼ���¼����
       initials.add(a);
       initials.add(b);
       classData[0] = data[a];
       classData[1] = data[b];
      
       //�ڽ�����½����ĳ���������Ķ��󣬲��ѳ�ʼ����ӽ�ȥ
       HashMap<Integer,Integer> resultOne = new HashMap<Integer,Integer>();
       HashMap<Integer,Integer> resultTwo = new HashMap<Integer,Integer>();
       resultOne.put(a,a);
       resultTwo.put(b,b);
       result.add(resultOne);
       result.add(resultTwo);
      
       //�ҵ�ʣ��ļ�����ʼ��
       while(alreadyCls < ClassCount)
       {
	      i = j = 0;
	      float maxMin = 0;
	      int newClass = -1;
	     
	      //����Сֵ�е����ֵ
	      for(;i < InstanceNumber;i++)
	      {
	          float min = 0;
	          float newMin = 0;
	          //�Һ����������Сֵ
	          if(initials.contains(i))
	        	  continue;
	          //������ȥ��
	          if(noises.containsKey(i))
	        	  continue;
	          for(j = 0;j < alreadyCls;j++)
	          {
	        	  newMin = calDis(data[i],classData[j]);
	        	  if(min == 0 || newMin < min)
	        		  min = newMin;
	          }
         
	          //����С����ϴ�
	          if(min > maxMin)
	          {
	        	  maxMin = min;
	        	  newClass = i;
	          }
	      }
	      //��ӵ���ֵ���Ϻͽ��������
	      //System.out.println("NewClass"+newClass);
	      initials.add(newClass);
	      classData[alreadyCls++] = data[newClass];
	      HashMap<Integer,Integer> rslt = new HashMap<Integer,Integer>();
	      rslt.put(newClass,newClass);
	      result.add(rslt);
       }
   }
   
   /**
    * ָ����ʼ�����ĵ�
    */
   public void setInitials()
   {
	   
   }
  
   //��һ�η���
   public void firstClassify()
   {
       //���ݳ�ʼ��������
       for(int i = 0;i < InstanceNumber;i++)
       {
    	   float min = 0f;
    	   int clsId = -1;
    	   for(int j = 0;j < classData.length;j++)
    	   {
    		   //ŷʽ����
    		   float newMin = calDis(classData[j],data[i]);
    		   if(clsId == -1 || newMin <min)
    		   {
    			   clsId = j;
    			   min = newMin;
    		   }
         
    	   }
    	   //���������
    	   if(!result.get(clsId).containsKey(i))
    		   result.get(clsId).put(i,i);
       	}
   	}
   	//�������ֱ࣬������������ݲ��ٱ仯
   	public void Adjust()
   	{
       //��¼�Ƿ����仯
       boolean change = true;
      
       //ѭ���Ĵ���
       int times = 1;
       while(change)
       {
    	   //��λ
    	   change = false;
    	   System.out.println("Adjust Iteration"+(times++)+"time(s)");
                   
    	   //���¼���ÿ����ľ�ֵ 
    	   for(int i = 0;i < ClassCount; i++){ 
    		   //ԭ�е����� 
    		   HashMap<Integer,Integer> cls = result.get(i); 
       
    		   //�µľ�ֵ 
    		   float[] newMean = new float[FieldCount ]; 
       
    		   //�����ֵ 
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
    	   //���֮ǰ������ 
    	   for(HashMap<Integer,Integer> cls:result) 
    		   cls.clear(); 
        
    	   //���·��� 
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
                 
         //���Ծ���Ч��(ѵ����) 
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
           * ����a������b������ŷʽ������Ϊ�����ƶ�
           * 
           * @param a     ����a
           * @param b     ����b
           * @return      ŷʽ���볤��
           */ 
   private float calDis(float[] aVector,float[] bVector)  {
      double dis = 0;
      int i = 0;//0λ��ŵ���ID
               /*���һ��������ѵ������Ϊ��������Բ�����  */
                for(;i < aVector.length;i++)
                     dis += Math.pow(bVector[i] - aVector[i],2); 
                dis = Math.pow(dis, 0.5); 
                return (float)dis; 
   }
         
        /**
         * �ж�������ֵ�����Ƿ����
         * 
         * @param a ����a
              * @param b ����b
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
         * ����������һ���ļ���
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
                  //д���ļ� 
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
               
               //ͳ��ÿ�����Ŀ����ӡ������̨ 
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
                 
               //�ر���Դ 
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

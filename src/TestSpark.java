import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Pattern;






























import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.aotain.common.CommonFunction;
import com.aotain.common.HFileConfigMgr;
import com.aotain.common.HfileConfig;
import com.aotain.common.HFileConfigMgr.FieldItem;
import com.hadoop.mapreduce.LzoTextInputFormat;

import scala.Tuple2;


public class TestSpark {
   private static final Pattern SPACE = Pattern.compile("\\|");
   public static void main(String[] args) {
	   System.out.println("0###OK################################");
	   String inputpath=args[0];
	   String outpath=args[1];
	   testSpark(inputpath,outpath);
	   System.out.println("1###OK################################");
}
   public static void testSpark(String ip,String outpath){
	    SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
	    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	 
	    Configuration config = new Configuration();
	    config.addResource("/etc/hadoop/conf");
	  
	 /*   Configuration config = new Configuration();
	    config.addResource("/etc/hadoop/conf");
	    
	   // System.out.println("#%%%HDFS:" + config.get("fs.defaultFS"));

        HFileConfigMgr configMgr = null;
        
        try {
			configMgr = new HFileConfigMgr(Config);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	HfileConfig confHfile = configMgr.config;*/
       final Map<String,String>webconfigmap=new HashMap<String, String>();
       webconfigmap.put("qq.com", "qq.com=o_cookie@;@12=2");
       webconfigmap.put(".360.c", ".360.c=mid@\\&@4=0");
       webconfigmap.put("www.baidu.com", "www.baidu.com=BAIDUID@;@12=1");
		final String fieldsplit = "\\|";
	    //JavaRDD<String> lines = ctx.textFile(ip, 1);
	    JavaPairRDD<LongWritable,Text> line=ctx.newAPIHadoopFile(ip,LzoTextInputFormat.class,  LongWritable.class, Text.class,config);
	    JavaPairRDD<String, String>userRecoder=line.filter(new Function<Tuple2<LongWritable,Text>, Boolean>() {
		
		@Override
		public Boolean call(Tuple2<LongWritable, Text> v1) throws Exception {
			  String[] items=v1._2.toString().split(fieldsplit,-1);
			  String systemour=items[6].toLowerCase().trim();  
			  boolean sysflag=systemour.equals("windows")||systemour.equals("macintosh")||systemour.equals("x11")||systemour.equals("linux");
			
			return sysflag;
		}
	  }).mapToPair(new PairFunction<Tuple2<LongWritable,Text>, String, String>() {

			@Override
			public Tuple2<String, String> call(Tuple2<LongWritable, Text> t)
					 {
				  String[] items=t._2.toString().split(fieldsplit,-1);
				  String domain=items[3];
				     String cookieValue="null";
					 String sys="null";
					 String keyvalue=null;
				      sys=items[6]+items[7];
		                    int index=-1;
						     if(domain.contains("qq.com")){
						    	 index=2;
						    	 cookieValue= parseCookie(cookieValue, webconfigmap, items, "qq.com");
						    	 cookieValue=CommonFunction.getQQNumber(cookieValue);
						     }else if(domain.contains(".360.c")){
						    	 index=0;
						    	 cookieValue= parseCookie(cookieValue, webconfigmap, items, ".360.c");
						     }else {
						    	 index=1;
						    	 cookieValue= parseCookie(cookieValue, webconfigmap, items,domain );
						     }
						     if(!"null".equals(cookieValue)||!"null".equals(sys))
						    	 keyvalue=items[3]+","+cookieValue+","+index+","+sys;
				
			           return new Tuple2<String, String>(items[1],keyvalue ) ;
			  
				
			}
		});
	    long count=0;
	   if(userRecoder.count()>0){
			    JavaRDD<String> outresult=userRecoder.groupByKey().map(new Function<Tuple2<String,Iterable<String>>, String>() {
		
					@Override
					public String call(Tuple2<String, Iterable<String>> v1) throws Exception {
						String date = "20150427";
					 	String vkey = v1._1().toString()+",";
					 	/*int max=0;*/
					 	try{
					 		//将网站分为三组，一组为常规的过滤网站放入webmap中，一组为qq类型网站，放入qqmap中；一组为统计360类网站，放入360map中
					 	 Map<String,Set<String>>webmap=new HashMap<String, Set<String>>();
					 	Map<String,Set<String>>qqmap=new HashMap<String, Set<String>>();
					 	Map<String,Set<String>>map360=new HashMap<String, Set<String>>();
					 	Set<String>systemset=new HashSet<String>();
					 	     if(v1._2()!=null){
					 	     Iterator<String> it=v1._2().iterator();
					 	     while(it.hasNext()){
					 		 String truevalue=it.next();
					 		 if(truevalue.split(",")!=null&&truevalue.split(",").length==4){
							 		 String mapkey=truevalue.split(",")[0];
							 		 String cookie=truevalue.split(",")[1];
							 		 String operasys=truevalue.split(",")[3];
							 		 if(!"null".equals(operasys))
							 		 systemset.add(operasys);
							 		 if(!"null".equals(cookie))
							 		 {
							 		 int index=Integer.parseInt(truevalue.split(",")[2]);
							 		if(index==1){
								 		 if(webmap.get(mapkey)!=null){
								 			 Set<String> set=webmap.get(mapkey);
								 		     set.add(cookie);
								 		 }else{
								 			 Set<String>set=new HashSet<String>();
								 			 set.add(cookie);
								 			 webmap.put(mapkey, set);
								 		 }
							 		 }else if(index==2){
							 			 mapkey="qqcount";
							 			 if(qqmap.get(mapkey)!=null){
								 			 Set<String> set=qqmap.get(mapkey);
								 		     set.add(cookie);
								 		 }else{
								 			 Set<String>set=new HashSet<String>();
								 			 set.add(cookie);
								 			 qqmap.put(mapkey, set);
								 		 }
							 		 }else if(index==0){
							 			if(map360.get(mapkey)!=null){
								 			 Set<String> set=map360.get(mapkey);
								 		     set.add(cookie);
								 		 }else{
								 			 Set<String>set=new HashSet<String>();
								 			 set.add(cookie);
								 			 map360.put(mapkey, set);
								 		 }
							 		 }
					 		 }
						  }
					 	 }
					 	 String webname="";
					 	 int webmax=0;
					 	 //统计常规类型中账户数
					 	 Iterator<Entry<String, Set<String>>>webit=webmap.entrySet().iterator();
					 	 while(webit.hasNext()){
					 		Entry<String, Set<String>> en=webit.next();
					 		if(webmax<en.getValue().size()){
					 			webmax=en.getValue().size();
					 		    webname=en.getKey();
					 		    }
					 	 }
					 	 int qqmax=0;
					 	 if(qqmap.get("qqcount")!=null)
					 	 qqmax=qqmap.get("qqcount").size();
					 	 String qqname="qqcount";
					 	// Iterator<Entry<String, Set<String>>>qqit=qqmap.entrySet().iterator();
					 	 //统计qq类型中账户数
					 	/* while(qqit.hasNext()){
					 		Entry<String, Set<String>> en=qqit.next();
					 		if(qqmax<en.getValue().size()){
					 			qqmax=en.getValue().size();
					 			qqname=en.getKey();
					 		   }
					 	 }*/
					 	int max360=0;
					 	String name360="";
					 	//统计360类型中账户数
					 	 Iterator<Entry<String, Set<String>>>it360=map360.entrySet().iterator();
					 	 while(it360.hasNext()){
					 		Entry<String, Set<String>> en=it360.next();
					 		if(max360<en.getValue().size()){
					 			max360=en.getValue().size();
					 			name360=en.getKey();
					 		   }
					 	 }
					 	 int countmax=0;//最终统计数
					 	 
					 	 if(qqmax!=0||max360!=0){
					 		 if(qqmax==0)//如果无qq账户数则直接取360账户数放入最终统计数中
					 			 countmax=max360;
					 		 else if(max360==0)//如果360账户数为0则直接取qq账户数
					 			 countmax=qqmax;
					 		 else{
					 			 countmax=qqmax>max360?qqmax:max360;//取qq与360账户数中最大者，放入最终统计数中
					 		 }
					 			
					 	 }
					 	 if(webmax!=0){
					 	     if(countmax!=0)
					 	     countmax=webmax>countmax?countmax:webmax;//将最终统计数与常规统计比较，取二者之中小的那一方
					 	     else
					 	    countmax=webmax;
					 	 }
					 	 int systemcount=systemset.size();
					 	countmax=countmax>systemcount?countmax:systemcount;
					 	 /* max=webmax>=qqmax?qqmax:webmax;
					 	 max=max>=max360?max360:max;*/
					 	 vkey+=date+","+webmax+","+webname+","+qqmax+","+qqname+","+max360+","+name360+","+systemcount+","+countmax+",";
					 	if(countmax!=0) 
		                  return vkey;
					 	//context.write(new Text(vkey), new Text(""));
					 	}
					 	     }catch(Exception e){
					 		e.printStackTrace();
					 	}
						return null;
					}
				}).filter(new Function<String, Boolean>() {
					
					@Override
					public Boolean call(String v1) throws Exception {
						
						return v1!=null;
					}
				});
			count=outresult.count(); 
			
			 List<String> output = outresult.collect();
			 
			    for (int i=0;i<100;i++) {
			      System.out.println(output.get(i));
			    }
			    outresult.saveAsTextFile(outpath);    
	    }
	    System.out.println("原始记录数:"+line.count()+"处理后记录数："+userRecoder.count()+"最终结果数："+count);
        /*JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
	      @Override
	      public Iterable<String> call(String s) {
	        return Arrays.asList(SPACE.split(s));
	      }
	    });*/
        
/*	    JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
	      @Override
	      public Tuple2<String, Integer> call(String s) {
	        return new Tuple2<String, Integer>(s, 1);
	      }
	    });
	    
	    JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
	      @Override
	      public Integer call(Integer i1, Integer i2) {
	        return i1 + i2;
	      }
	    });
*/
	    
	    ctx.stop();
	   
   }
  private static String parseCookie(String cookieValue,Map <String,String>webconfigmap,String[] items,String key){
       
  	 String webconfigStr=webconfigmap.get(key);
	    if(webconfigStr!=null){
		    	 String[] webconfigs=webconfigStr.split("=",3);
		    	 String [] configparse=webconfigs[1].split("@");
		    	 int position=Integer.parseInt(configparse[2]);
		    	 String startname=configparse[0];
				 String endtname=configparse[1];
				  //index=Integer.parseInt(webconfigs[2]);
				 if(items[position].length()>0&&items[position].indexOf(startname)!=-1){
					  cookieValue=items[position].substring(items[position].indexOf(startname),items[position].length());
					  if(cookieValue.indexOf(endtname)!=-1)
					  cookieValue=cookieValue.substring(0,cookieValue.indexOf(endtname));
					  if(cookieValue.split("=").length==2)
					  cookieValue=cookieValue.split("=")[1];
				 }
	    	 }
	  return cookieValue;
}
}

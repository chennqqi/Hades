package com.aotain.project.dmp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnalysisDataLabelMapper extends
		Mapper<NullWritable, OrcStruct, Text, Text> {
	public Map<String, String> otherDomainMap = new HashMap<String, String>();
	public Map<String, String> domainMap = new HashMap<String, String>();
	public Map<String, String> kewordMap = new HashMap<String, String>();
	HashMap<String, String> classMap = new HashMap<String, String>();
	Map<String, String> choosekeywordMap = new HashMap<String, String>();
	public Map<String,String>  carMap=new HashMap<String, String>();
	public Map<String,String>  specialbvMap=new HashMap<String, String>();
	public Map<String, String> carIndustryMap = new HashMap<String, String>();
	public Map<String, String> mohucarIndustryMap = new HashMap<String, String>();
	Map<String,Map<String,String>>webmap=new HashMap<String, Map<String,String>>();
	static StructObjectInspector inputOI;
	final static String inputSchema = "struct<areaid:string,username:string,srcip:string,domain:string,"
			+ "url:string,refer:string,opersys:string,opersysver:string,browser:string,"
			+ "browserver:string,device:string,accesstime:bigint,cookie:string,keyword:string,"
			+ "urlclassid:string,referdomain:string,referclassid:string,ua:string,"
			+ "destinationip:string,sourceport:string,destinationport:string>";
    public void setWebKeyWordConfig(Context context){
    	String fileName = "/user/project/dmp/webkey_config";
		try {

			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(URI.create(fileName), conf);
			FSDataInputStream in = null;
			BufferedReader fis = null;

			try {
				Path path = new Path(fileName);
				if (fs.exists(path)) {
					System.out.println(fileName);
					System.out.println("exist conf file !!!!!!");
					if (fs.isDirectory(path)) {
						for (FileStatus file : fs.listStatus(path)) {
							System.out.println(fileName);
							System.out.println("exist conf file !!!!!!");
							Path filepath = file.getPath();
							String filename = filepath.getName();
							in = fs.open(filepath);
							fis = new BufferedReader(new InputStreamReader(in,
									"UTF-8"));
							// fis = new BufferedReader(new
							// FileReader(fileName));
							String pattern = null;
							// int i = 0;
							while ((pattern = fis.readLine()) != null) {
								String[] data= pattern.split("#@@@#", -1);
								String website=data[0];
								String key_values[]=data[1].split(",");
								Map<String,String>key_labels=new HashMap<String, String>();
								for(String key_value:key_values){
									String key=key_value.split("#")[0];
									String value=key_value.split("#")[1];
									key_labels.put(key, value);
								}
								webmap.put(website, key_labels);
							}
						}
					}
				}
			} catch (IOException ioe) {
				System.err
						.println("Caught exception while parsing the  file '");
				// ioe.printStackTrace();
			} finally {
				if (in != null) {
					in.close();
				}
				if (fis != null) {
					fis.close();
				}
			}
		} catch (Exception e) {
			// e.printStackTrace();
		}
    }
	public void setChooseKeyword() {
		choosekeywordMap.put("anjuke.com", "1,1,1");
		choosekeywordMap
				.put("anjuke.com/loupan",
						"(http://[a-z]+\\.fang\\.anjuke\\.com/loupan/[A-Za-z_0-9]*/?[-_?&%=+A-Za-z0-9]*),"
								+ "sz.fang.anjuke.com/loupan/|dg.fang.anjuke.com/loupan/|hui.fang.anjuke.com/loupan/|zs.fang.anjuke.com/loupan/,"
								+ "html|ajax|?from|huxing|?pi=");
		choosekeywordMap
				.put("anjuke.com/sale",
						"1,"
								+ "shenzhen.anjuke.com/sale/|dg.anjuke.com/sale/|huizhou.anjuke.com/sale/|zs.anjuke.com/sale/,?pi|?from");
		choosekeywordMap.put("lianjia.com", "1,1,1");
		choosekeywordMap.put("lianjia.com/loupan",
				"(http://sz\\.fang\\.lianjia\\.com/loupan/[A-Za-z]*/?[li0-9]*/?[A-Za-z%0-9]*),"
						+ "sz.fang.lianjia.com/loupan/,p\\_|kaifashang");
		choosekeywordMap
				.put("lianjia.com/ershoufang",
						"(http://sz\\.lianjia\\.com/ershoufang/[0-9A-Za-z%/?=]+),"
								+ "1,sz.lianjia.com/ershoufang/housestat|sz.lianjia.com/ershoufang/?utm");
		choosekeywordMap.put("fang.com", "2,1,1");
		choosekeywordMap
				.put("newhouse.fang.com",
						"2,"
								+ "newhouse.huizhou.fang.com/house/s/|newhouse.dg.fang.com/house/s/|newhouse.sz.fang.com/house/s/|newhouse.zs.fang.com/house/s/"
								+ ",?mapmode=|?strDistrict=|?a=get");
		choosekeywordMap
				.put("esf.fang.com",
						"2,"
								+ "esf.sz.fang.com/house|esf.dg.fang.com/house|esf.huizhou.fang.com/house|esf.zs.fang.com/house"
								+ ",1");
		choosekeywordMap.put("qfang.com", "1,1,1");
		choosekeywordMap
				.put("qfang.com/newhouse",
						"(http://[a-z]+\\.qfang\\.com/newhouse/list[-/a-zA-Z0-9]*\\?keyword=[%A-Za-z0-9]+),"
								+ "shenzhen.qfang.com/newhouse/list|dongguan.qfang.com/newhouse/list|huizhou.qfang.com/newhouse/list|huizhou.qfang.com/newhouse/list,"
								+ "p\\_|kaifashang");
		choosekeywordMap
				.put("qfang.com/sale",
						"(http://[a-z]+\\.qfang\\.com/sale/[-a-z]+/?[-a-z0-9]*),"
								+ "shenzhen.qfang.com/sale/|dongguan.qfang.com/sale/|huizhou.qfang.com/sale|zhongshan.qfang.com/sale/,"
								+ "ajax|trend|ask|recommend|detail");
	}

	public void setClassMap(Context context) throws IOException {
		String confuri = "/user/hive/warehouse/dmp.db/url_class/";
		Configuration config = context.getConfiguration();

		// IPCYW9971632,218.17.90.26,218.17.90.62,255.255.255.192,
		FileSystem fs = FileSystem.get(URI.create(confuri), config);
		FSDataInputStream in = null;
		Path path = new Path(confuri);
		if (fs.exists(path)) {
			System.out.println(confuri);
			System.out.println("exist conf file !!!!!!");
			try {
				if (fs.isDirectory(path)) {
					for (FileStatus file : fs.listStatus(path)) {
						System.out.println(confuri);
						System.out.println("exist conf file !!!!!!");
						try {
							// file.readFields(in);
							in = fs.open(file.getPath());
							BufferedReader bis = new BufferedReader(
									new InputStreamReader(in, "UTF8"));
							String line = "";
							while ((line = bis.readLine()) != null) {
								String cols[] = line.split(",", -1);
								if (cols.length > 3) {
									String domain = new URL(cols[3]).getHost();
									classMap.put(domain, cols[0] + "_"
											+ cols[1]);
								}
								// IP,ADSL
							}
							// deviceList.deleteCharAt(deviceList.length()-1);//ȥ�����һ������
							// System.out.println(deviceList.toString());

						} finally {
							IOUtils.closeStream(in);
						}
					}
				}
			} finally {
				IOUtils.closeStream(in);
			}
		} else {
			System.out.println(confuri);
			System.out.println("not exist file !!!!!!");
		}
	}

	private void setDomainValue() {
		domainMap
				.put("anjuke.com",
						"anjuke,http://[a-z]+\\.fang\\.anjuke\\.com/loupan/[A-Za-z\\-]*([0-9]+)\\.html\\??,1");
		// |http://[a-z]+\\.fang\\.anjuke\\.com/loupan/[A-Za-z]+-([0-9]+)\\.html\\?from=loupan_tab
		domainMap.put("fang.com/bbs",
				"soufang,http://([A-Za-z0-9]+)\\.fang\\.com/bbs/,1");
		domainMap
				.put("fang.com/house",
						"soufang,http://([A-Za-z0-9]+)\\.fang\\.com/house/yh[0-9_]+\\.htm$|http://([A-Za-z0-9]+)\\.fang\\.com/house/[0-9]+/housedetail.htm$|http://([A-Za-z0-9]+)\\.fang\\.com/house/[0-9]+/fangjia.htm$|http://([A-Za-z0-9]+)\\.fang\\.com/house/[0-9]+/dongtai.htm$,1");
		domainMap
				.put("fang.com/photo",
						"soufang,http://([A-Za-z0-9]+)\\.fang\\.com/photo/[A-Za-z0-9_]+\\.htm$^?,1");
		domainMap.put("fang.com/dianping",
				"soufang,http://([A-Za-z0-9]+)\\.fang\\.com/dianping/$,1");
		domainMap.put("fang.com/zhuangxiu",
				"soufang,http://([A-Za-z0-9]+)\\.fang\\.com/zhuangxiu/$,1");
		domainMap.put("fang.com",
				"soufang,^http://([A-Za-z0-9]+)\\.fang\\.com$,2");
		domainMap
				.put("lianjia.com",
						"lianjia,http://[a-z]+\\.fang\\.lianjia\\.com/loupan/p_([A-Za-z0-9]+)/?\\??,1");
		domainMap
				.put("qfang.com",
						"qfang,http://[a-z]+\\.qfang\\.com/newhouse/[A-Za-z]+/([0-9]+)\\??,1");
	}

	private void setKeyWordRule(Context context) {
		String fileName = "/user/project/dmp/config";
		try {

			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(URI.create(fileName), conf);
			FSDataInputStream in = null;
			BufferedReader fis = null;

			try {
				Path path = new Path(fileName);
				if (fs.exists(path)) {
					System.out.println(fileName);
					System.out.println("exist conf file !!!!!!");
					if (fs.isDirectory(path)) {
						for (FileStatus file : fs.listStatus(path)) {
							System.out.println(fileName);
							System.out.println("exist conf file !!!!!!");
							Path filepath = file.getPath();
							String filename = filepath.getName();
							in = fs.open(filepath);
							fis = new BufferedReader(new InputStreamReader(in,
									"UTF-8"));
							// fis = new BufferedReader(new
							// FileReader(fileName));
							String pattern = null;
							// int i = 0;
							while ((pattern = fis.readLine()) != null) {
								String cols[] = pattern.split("\\|");
								String value = cols[1];
								if (value.indexOf(":") != -1)
									value = value.substring(0,
											value.indexOf(":"));
								

								kewordMap.put(filename + cols[0], value);
								System.out.println(cols[0]);
							}
						}
					}
				}
			} catch (IOException ioe) {
				System.err
						.println("Caught exception while parsing the  file '");
				// ioe.printStackTrace();
			} finally {
				if (in != null) {
					in.close();
				}
				if (fis != null) {
					fis.close();
				}
			}
		} catch (Exception e) {
			// e.printStackTrace();
		}

	}

	private void setOtherDomainValue(Context context) {
//		otherDomainMap
//				.put("58.com",
//						"二手房,(http://sz\\.58\\.com/ershoufang/[0-9]+x\\.shtml)\\??|(http://sz\\.58\\.com/[a-z]*/?ershoufang/?[0-9a-z]*)\\??,1");
//		otherDomainMap.put("ifeng.com", "新房,1,2");
//		otherDomainMap
//				.put("ifeng.com/sale",
//						"新房,(http://sz\\.house\\.ifeng\\.com/sale/search/36688/_/_/[0-9_]+\\.shtml)\\??,1");
//		otherDomainMap.put("ifeng.com/homedetail",
//				"新房,(http://sz\\.house\\.ifeng\\.com/homedetail/[0-9]+)\\??,1");
//		otherDomainMap
//				.put("ifeng.com/world",
//						"海外房产,(http://world\\.house\\.ifeng\\.com/world/[A-Z]+/_/_/[0-9_]+\\.shtml)\\??|(http://world\\.house\\.ifeng\\.com/world/homedetail/[0-9]+)\\??,1");
//		otherDomainMap
//				.put("ifeng.com/detail",
//						"海外房产,(http://world\\.house\\.ifeng\\.com/detail/[0-9_]+/[0-9_]+\\.shtml)\\??|(http://sz\\.house\\.ifeng\\.com/detail/[0-9_]+/[0-9_]+\\.shtml)\\??,1");
//
//		otherDomainMap.put("centanet.com", "新房,1,2");
//		otherDomainMap
//				.put("centanet.com/xinfang",
//						"新房,(http://sz\\.centanet\\.com/xinfang/lp-[0-9A-Z]+/?)\\??|(http://sz\\.centanet\\.com/xinfang/[a-z]*/?[0-9a-z]*/?)\\??,1");
//		otherDomainMap
//				.put("centanet.com/ershoufang",
//						"二手房,(http://sz\\.centanet\\.com/ershoufang/[-0-9a-z]+\\.html)\\??|(http://sz\\.centanet\\.com/ershoufang/[a-z]*/?[0-9a-z]*/?)\\??,1");
//		otherDomainMap
//				.put("centanet.com/xzl",
//						"新房,(http://sz\\.centanet\\.com/xzl/BusinessShop/Search/[-_%0-9a-zA-Z]+)\\??|(http://sz\\.centanet\\.com/xzl/BusinessShop/[dD]+etails/[-0-9a-z]+)\\??,1");
//		otherDomainMap
//				.put("centanet.com/xiaoqu",
//						"二手房,(http://sz\\.centanet\\.com/xiaoqu/[a-z]*/?[0-9a-z]*/?)\\??|(http://sz\\.centanet\\.com/xiaoqu/[a-z]+-[a-z]+/?)\\??,1");
//		otherDomainMap.put("qq.com", "新房,1,3");
//		otherDomainMap.put("db.house.qq.com",
//				"新房,(http://db\\.house\\.qq\\.com/sz_[0-9]+/?)\\??,1");
//		otherDomainMap
//				.put("esf.db.house.qq.com",
//						"二手房,(http://esf\\.db\\.house\\.qq\\.com/sz/sale/[0-9]+_[0-9]+/?)\\??,1");
//		otherDomainMap.put("ganji.com", "新房,1,2");
//		otherDomainMap
//				.put("ganji.com/fang5",
//						"新房,(http://sz\\.ganji\\.com/fang5/[0-9]+x\\.htm)\\??|(http://sz\\.ganji\\.com/fang5/[a-z]*/?[a-z0-9]*/?)\\??,1");
//		otherDomainMap.put("163.com", "新房,1,3");
//		otherDomainMap
//				.put("xf.house.163.com",
//						"新房,(http://xf\\.house\\.163.com/sz/search/[0-9-]+\\.html)\\??|(http://xf\\.house\\.163.com/sz/[A-Z0-9]+\\.html)#?,1");
//		otherDomainMap
//				.put("vanke.com",
//						"新房,(http://life\\.vanke\\.com/web/goodsDetail\\?GoodsID=[0-9]+&GoodsType=[0-9]+)&?,1");
//		otherDomainMap.put("jjshome.com", "新房,1,2");
//		otherDomainMap
//				.put("jjshome.com/ysl",
//						"新房,(http://shenzhen\\.jjshome\\.com/ysl/[0-9]+)\\??|(http://shenzhen\\.jjshome\\.com/ysl/index/[0-9a-z]+)\\??,1");
//		otherDomainMap
//				.put("jjshome.com/esf",
//						"二手房,(http://shenzhen\\.jjshome\\.com/esf/[0-9]+)\\??|(http://shenzhen\\.jjshome\\.com/esf/index/[0-9a-z]+)\\??,1");
//		otherDomainMap.put("fangdd.com", "新房,1,3");
//
//		otherDomainMap.put("m.fangdd.com", "新房,(http://m.fangdd.com/nh),1");
//		otherDomainMap
//				.put("hd.xf.fangdd.com", "新房,(http://hd.xf.fangdd.com),1");
//		otherDomainMap.put("fs.esf.fangdd.com",
//				"二手房,(http://fs.esf.fangdd.com),1");
//		otherDomainMap
//				.put("xf.fangdd.com",
//						"新房,(http://xf\\.fangdd\\.com/[a-z]+/[0-9]+\\.html)\\??|(http://xf\\.fangdd\\.com/shenzhen/loupan/[-0-9a-z]+)\\??,1");
//		otherDomainMap
//				.put("esf.fangdd.com",
//						"二手房,(http://esf\\.fangdd\\.com/[a-z]+/[0-9]+\\.html)\\??|(http://esf\\.fangdd\\.com/shenzhen/list/[-_%A-Z0-9a-z]+)\\??,1");
//		otherDomainMap.put("baidu.com", "新房,1,3");
//		otherDomainMap
//				.put("fang.baidu.com",
//						"新房,(http://fang\\.baidu\\.com/secondhouse\\?uid=[0-9]+&uid_type=house&estate_id=[0-9]+&city_id=340)\\??,1");
//		otherDomainMap.put("szhome.com", "新房,1,2");
//		otherDomainMap.put("szhome.com/house",
//				"新房,(http://m\\.leju\\.com/touch/house/sz/[0-9]+/?)\\??,1");
//		otherDomainMap.put("szhome.com/sell",
//				"二手房,(http://esf\\.szhome\\.com/sell/[0-9]+\\.html)\\??,1");
//		otherDomainMap.put("leju.com", "新房,1,4");
//		otherDomainMap.put("leju.com/touch/house",
//				"新房,(http://m\\.leju\\.com/touch/house/sz/[0-9]+/?)\\??,1");
//		otherDomainMap
//				.put("leju.com/touch/esf",
//						"二手房,(http://m\\.leju\\.com/touch/esf/sz/detail/[0-9]+)\\??|(http://m\\.leju\\.com/touch/esf/sz/detail/[-0-9a-z]+/?)\\??,1");
//		otherDomainMap.put("anjuke.com", "新房,1,5");
//		otherDomainMap.put("api.anjuke.com",
//				"新房,(http://api.anjuke.com/mobile/v5/recommend/sale),1");
//		otherDomainMap.put("m.anjuke.com",
//				"海外房产,(http://m.anjuke.com/haiwai),1");
//		otherDomainMap
//				.put("s.anjuke.com",
//						"海外房产,(http://s.anjuke.com/st?__site=m_anjuke&p=Haiwai)|(http://s.anjuke.com/st?__site=m_anjuke-npv&p=Haiwai),1");
//		otherDomainMap
//				.put("api.fang.anjuke.com",
//						"新房,(http://api.fang.anjuke.com/m/android/1.3/loupan/newlistv2)|(http://api.fang.anjuke.com/m/android/1.3/loupan/Newimages)|(http://api.fang.anjuke.com/m/android/1.3/loupan/lessGussLike),5");
//		otherDomainMap
//				.put("anjuke.com/haiwai",
//						"海外房产,(http://[a-z]+\\.haiwai\\.anjuke\\.com/detail/building-[0-9]+\\.html)\\??,1");
//		otherDomainMap.put("fang.com", "新房,1,5");
//		otherDomainMap
//				.put("fang.com/house",
//						"新房,(http://[0-9a-z]+\\.fang\\.com/house/[0-9_]+\\.htm)\\??|(http://[a-z]+\\.fang\\.com/house/[0-9]+\\.htm)\\??,1");
//		otherDomainMap.put("shop.fang.com",
//				"商业地产,(http://shop\\.fang\\.com/shou/[0-9_]+\\.html)\\??,1");
//		otherDomainMap.put("office.fang.com",
//				"商业地产,(http://office\\.fang\\.com/zu/[0-9_]+\\.html)\\??,1");
//		otherDomainMap
//				.put("m.fang.com",
//						"新房,(http://m.fang.com/world)|(http://m.fang.com/fangjia/?c=pinggu&a=worldFangjia&src=client]),1");
//		otherDomainMap
//				.put("soufunapp.3g.fang.com",
//						"新房,(http://soufunapp.3g.fang.com/http/sf2014.jsp?city=%E6%B7%B1%E5%9C%B3&location=newhouse)|(http://soufunapp.3g.fang.com/http/sfservice.jsp?channel=houselist&city=%E6%B7%B1%E5%9C%B3&housetype=xf)|(http://soufunapp.3g.fang.com/http/sfservice.jsp?city=%E6%B7%B1%E5%9C%B3&fromType=1&messagename=getNewHousePriceData),1");
//		otherDomainMap.put("soufunappxf.3g.fang.com",
//				"新房,(http://soufunappxf.3g.fang.com),1");
//		otherDomainMap.put("lianjia.com", "新房,1,5");
//		otherDomainMap
//				.put("lianjia.com/house",
//						"海外房产,(http://[a-z]+\\.lianjia\\.com/house/[A-Z0-9]+\\.html)\\??,1");
//		otherDomainMap
//				.put("image2.lianjia.com",
//						"新房,(http://image2.lianjia.com/newhcms)|(http://image2.lianjia.com/x-xf),1");
//		otherDomainMap.put("image3.lianjia.com",
//				"新房,(http://image3.lianjia.com/xf-resblock),1");
//		otherDomainMap.put("qfang.com", "新房,1,6");
//		otherDomainMap.put("qfang.com/appapi/v4_2/newHouse",
//				"新房,(http://shenzhen.qfang.com/appapi/v4_2/newHouse),1");
//		otherDomainMap.put("qfang.com/appapi/v4_2/room",
//				"二手房,(http://shenzhen.qfang.com/appapi/v4_2/room),1");
//		otherDomainMap.put("qfang.com/appapi/v4_2/office",
//				"商业地产,(http://shenzhen.qfang.com/appapi/v4_2/office),1");
//		otherDomainMap.put("qfang.com/appapi/v4_2/enums",
//				"商业地产,(http://shenzhen.qfang.com/appapi/v4_2/enums),1");
//		otherDomainMap
//				.put("qfang.com/office/rent",
//						"商业地产,(http://shenzhen\\.qfang\\.com/office/rent/[0-9]+)\\??,1");
//		otherDomainMap
//				.put("qfang.com/office/sale",
//						"商业地产,(http://shenzhen\\.qfang\\.com/office/sale/[0-9]+)\\??,1");
		
		String fileName = "/user/project/dmp/domainconfig/DMP_LABEL_URL.TXT";
		try {

			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(URI.create(fileName), conf);
			FSDataInputStream in = null;
			BufferedReader fis = null;

			try {
				Path path = new Path(fileName);
				
					
						
							String filename = path.getName();
							in = fs.open(path);
							fis = new BufferedReader(new InputStreamReader(in,
									"UTF-8"));
							// fis = new BufferedReader(new
							// FileReader(fileName));
							String pattern = null;
							// int i = 0;
							while ((pattern = fis.readLine()) != null) {
								String cols[] = pattern.split("#@@@#");
								int flag=Integer.parseInt(cols[6]);
								prase(flag, otherDomainMap, cols);
								

							}
					
					
				
			} catch (IOException ioe) {
				System.err
						.println("Caught exception while parsing the  file '");
				// ioe.printStackTrace();
			} finally {
				if (in != null) {
					in.close();
				}
				if (fis != null) {
					fis.close();
				}
			}
		} catch (Exception e) {
			// e.printStackTrace();
		}
	}
	
	public String prase(int chooseFlag, Map<String,String> otherDomainMap,String cols[]){
		String key="";
		 
		try {
			 String url=cols[7];
			 URL urlt = new URL(url); 
			 String domain=urlt.getHost();
			 String domains[] = domain.split("\\.");
			 int domainLength = domains.length;
			 if (domainLength > 1) {
					String rootDomain = domains[domains.length - 2] + "."
							+ domains[domains.length - 1];
					String value=cols[1]+","+cols[4]+",1,match,"+cols[3]+","+cols[0];
					if(chooseFlag==3){
						key=domain;
						
						
						otherDomainMap.put(key, value);
					}else {
						otherDomainMap.put(rootDomain, "新房,1,2,match,"+cols[3]+","+cols[0]);
						String paths[] = new URL(url).getPath().split("/");
							if(chooseFlag==2&&paths.length > 1){
							  
								   key = rootDomain + "/" + paths[1];
								 
						}else if(chooseFlag==4&&paths.length > 2){
							 key = rootDomain + "/" + paths[1] + "/"+ paths[2];
						}else if(chooseFlag==5){
							key=domain;
							if (paths.length > 1) {
								rootDomain = rootDomain + "/" + paths[1];
							  }
							key=key+"|"+rootDomain;
						}else if(chooseFlag==6&&paths.length > 2){
							key = rootDomain + "/" + paths[1] + "/"+ paths[2];
								if(paths.length > 3){
									rootDomain = rootDomain + "/" + paths[1] + "/"+ paths[2] + "/" + paths[3];
									key=key+"|"+rootDomain;	
								}
						}   
							String keys[]=key.split("\\|");
							for(int i=0;i<keys.length;i++){
							otherDomainMap.put(keys[i], value);
							}
					}
					
					}
		
			
			
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return key;
	}
	private void setCarRule(){
	    	carMap
			.put("autohome.com.cn",
					"zhijia,http://www.autohome.com.cn/spec/([0-9]+)|http://cars\\.app\\.autohome\\.com\\.cn/.*specids=([0-9]+),1");
	    	carMap
			.put("pcauto.com.cn",
					"taipingyang,http://price.pcauto.com.cn/m([0-9]+)|http://mrobot\\.pcauto\\.com\\.cn/.*modelId=([0-9]+),1");
	    	carMap
			.put("xcar.com.cn",
					"aika,http://newcar.xcar.com.cn/m([0-9]+),1");
	    	carMap
			.put("bitauto.com",
					"yiche,http://car.bitauto.com/[A-Za-z0-9\\-]+/m([0-9]+),1");
	    	carMap
			.put("yiche.com",
					"yiche,http://carapi\\.ycapp\\.yiche\\.com/.*carid=([0-9]+),1");
	    	
	    	carMap
			.put("sohu.com",
					"sohu,http://db.auto.sohu.com/[a-zA-Z0-9\\-]+/[0-9]+/([0-9]+)|http://open\\.dealer\\.auto\\.sohu\\.com/api/.*seriseId=([0-9]+),1");
	    	
	    }
	
	private void setSpecialBv(){
		
		specialbvMap.put("car.autohome.com.cn_对比","http://car.autohome.com.cn/duibi/[a-z]+/carids=[0-9]+,[0-9]+,[0-9]+,[0-9]+,0");
		specialbvMap.put("dealer.autohome.com.cn_试驾","http://dealer.autohome.com.cn/[0-9a-z]+/drive_,0");
		specialbvMap.put("dealer.autohome.com.cn_询价","http://dealer.autohome.com.cn/[0-9a-z]+/order(-|_),1");
		specialbvMap.put("account.autohome.com.cn_登陆","http://account.autohome.com.cn/?backurl=,0");
		specialbvMap.put("account.autohome.com.cn_注册","http://account.autohome.com.cn/register,0");
		specialbvMap.put("cars.app.autohome.com.cn_登陆","http://cars.app.autohome.com.cn/,0");
		
		specialbvMap.put("price.pcauto.com.cn_对比","http://price.pcauto.com.cn/((comment/)?choose(_photo)?.jsp\\?mid=[0-9]+|pk/sid(-([0-9]+))+.html),0");
		specialbvMap.put("price.pcauto.com.cn_试驾","http://price.pcauto.com.cn/(dealer/commorders/order.jsp\\?(t=2)?|[0-9]+/order2-),0");
		specialbvMap.put("price.pcauto.com.cn_询价","http://price.pcauto.com.cn/(dealer/commorders/order\\.jsp\\?(t=0)?|[0-9]+/order0-),0");
		specialbvMap.put("my.pcauto.com.cn_登录","http://my.pcauto.com.cn/login.jsp,0");
		specialbvMap.put("my.pcauto.com.cn_注册","^http://my.pcauto.com.cn/passport/mobileRegister.jsp$,0");
		specialbvMap.put("price.pcauto.com.cn_登录","http://mrobot.pcauto.com.cn/,0");
		
		specialbvMap.put("car.bitauto.com_对比","http://car.bitauto.com/[a-z]*duibi/(([0-9]+-[0-9]+/)|(\\?(carIDs|carids)=)),1");
		specialbvMap.put("dealer.bitauto.com_试驾","http://dealer.bitauto.com/([0-9]+/)?shijia/,0");
		specialbvMap.put("dealer.bitauto.com_询价","http://dealer.bitauto.com/([0-9]+/)?zuidijia/,0");
		specialbvMap.put("i.yiche.com_登录","http://i.yiche.com/authenservice/login.html,0");
		specialbvMap.put("i.yiche.com_注册","^http://i.yiche.com/authenservice/RegisterSimple/MobileRegister.html$,0");
		specialbvMap.put("carapi.ycapp.yiche.com_登录","http://carapi.ycapp.yiche.com/,0");
		
		specialbvMap.put("db.auto.sohu.com_对比","http://db.auto.sohu.com/(pk-trim|pk_trimpic).shtml(#((?!(0,0,0,0,0))([0-9]+,[0-9]+,[0-9]+,[0-9]+,[0-9]+))|\\?),0");
		specialbvMap.put("dealer.auto.sohu.com_试驾","http://dealer.auto.sohu.com/[0-9]+/order(/)?\\?.*type=try,0");
		specialbvMap.put("dealer.auto.sohu.com_询价","http://dealer.auto.sohu.com/([0-9]+/order\\?(type=ask)?|xunjia/),0");
		specialbvMap.put("i.auto.sohu.com_登录","http://i.auto.sohu.com/user/login/,0");
		specialbvMap.put("i.auto.sohu.com_注册","http://i.auto.sohu.com/user/register/,0");
		specialbvMap.put("open.dealer.auto.sohu.com_登录","http://open.dealer.auto.sohu.com/,0");
	
		
		
		specialbvMap.put("newcar.xcar.com.cn_对比","http://newcar.xcar.com.cn/compare(-photo)?/[0-9]+,0");
		specialbvMap.put("newcar.xcar.com.cn_试驾","http://newcar.xcar.com.cn/auto/index.php\\?r=dealerPopw/order.*type=2,0");
		specialbvMap.put("newcar.xcar.com.cn_询价","http://newcar.xcar.com.cn/auto/index.php\\?r=dealerPopw/order.*(type=1|mid=),0");
		specialbvMap.put("reg.xcar.com.cn_注册","^http://reg.xcar.com.cn/register.php$,0");
		

	}  
	
	private void setCarIndustryRule(){

		carIndustryMap.put("www.autohome.com.cn","新车,(http://www\\.autohome\\.com\\.cn/spec/[0-9]+/?)\\??,2");
		carIndustryMap.put("price.pcauto.com.cn","新车,(http://price\\.pcauto\\.com\\.cn/m[0-9]+/?)\\??,2");
		carIndustryMap.put("car.bitauto.com","新车,(http://car\\.bitauto\\.com/[a-z]+-*[a-z]+/m[0-9]+/?)\\??,2");
		carIndustryMap.put("newcar.xcar.com.cn","新车,(http://newcar\\.xcar\\.com\\.cn/m[0-9]+/?)\\??,2");
		carIndustryMap.put("db.auto.sohu.com","新车,(http://db\\.auto\\.sohu\\.com/[0-9a-z]+-*[0-9a-z]+/[0-9]+/[0-9a-z]+/?)\\??,2");
		carIndustryMap.put("data.auto.sina.com.cn","新车,(http://data\\.auto\\.sina\\.com\\.cn/car/?)\\??,2");
		carIndustryMap.put("data.auto.qq.com","新车,(http://data\\.auto\\.qq\\.com/car_models/?)\\??,2");
		carIndustryMap.put("auto.chexun.com","新车, 1,1");
		carIndustryMap.put("product.cheshi.com","新车,1,1");
		carIndustryMap.put("product.auto.163.com","新车,1,1");
		carIndustryMap.put("car.auto.ifeng.com","新车,1,1");
		carIndustryMap.put("car.autofan.com.cn","新车,1,1");	
		
		carIndustryMap.put("2sc.sohu.com","二手车,(http://2sc\\.sohu\\.com/[a-z]+-*[a-z]+/(buycar|[a-z]+-*[a-z]+)/?)\\??,2");
		carIndustryMap.put("used.xcar.com.cn","二手车,(http://used\\.xcar\\.com\\.cn/(search|shop)/?)\\??,2");
//		carIndustryMap.put("taoche.com","二手车,(http://[a-z]+\\.taoche\\.com/buycar/?)\\??,3");
		carIndustryMap.put("www.che168.com","二手车,(http://www\\.che168\\.com/dealer/?)\\??,2");
//		carIndustryMap.put("273.cn","二手车,(http://[a-z]+\\.273\\.cn/?)\\??,3");
//		carIndustryMap.put("baixing.com","二手车,(http://[a-z]+\\.baixing\\.com/ershouqiche/?)\\??,3");
		carIndustryMap.put("www.51auto.com","二手车,1,1");
		carIndustryMap.put("www.che168.com","二手车,1,1");
		carIndustryMap.put("www.hx2car.com","二手车,1,1");
		carIndustryMap.put("www.cn2che.com","二手车,1,1");
		carIndustryMap.put("www.carking001.com","二手车,1,1");
		carIndustryMap.put("www.guazi.com","二手车,1,1");
		carIndustryMap.put("www.xin.com","二手车,1,1");
		carIndustryMap.put("usedcar.auto.sina.com.cn","二手车,1,1");
		carIndustryMap.put("ucar.qq.com","二手车,1,1");

		mohucarIndustryMap.put("baixing.com","二手车,(http://[a-z]+\\.baixing\\.com/ershouqiche/?)\\??,3");
		mohucarIndustryMap.put("273.cn","二手车,(http://[a-z]+\\.273\\.cn/?)\\??,3");
		mohucarIndustryMap.put("taoche.com","二手车,(http://[a-z]+\\.taoche\\.com/buycar/?)\\??,3");

		carIndustryMap.put("www.pcauto.com.cn","汽车保养,(http://www\\.pcauto\\.com\\.cn/drivers/?)\\??,2");
		carIndustryMap.put("yanghu.bitauto.com","汽车保养,1,1");
		carIndustryMap.put("yp.xcar.com.cn","汽车保养,1,1");
		carIndustryMap.put("bbs.modiauto.com.cn","汽车保养,1,1");
		carIndustryMap.put("www.qcyhw.com","汽车保养,1,1");
		carIndustryMap.put("www.chebao360.com","汽车保养,1,1");
		carIndustryMap.put("www.360huche.com","汽车保养,1,1");
		carIndustryMap.put("www.all2car.com","汽车保养,1,1");
		carIndustryMap.put("www.tuhu.cn","汽车保养,1,1");
		
		carIndustryMap.put("www.xcar.com.cn","汽车论坛,(http://www\\.xcar\\.com\\.cn/bbs/?)\\??,2");
		carIndustryMap.put("bbs.fblife.com","汽车论坛,1,1");
		carIndustryMap.put("club.xcar.com.cn","汽车论坛,1,1");
		carIndustryMap.put("motorcycle.sh.cn","汽车论坛,1,1");
		carIndustryMap.put("bbs.360che.com","汽车论坛,1,1");
		carIndustryMap.put("www.mychery.net","汽车论坛,1,1");
		carIndustryMap.put("www.769car.com","汽车论坛,1,1");
		carIndustryMap.put("club.autohome.com.cn","汽车论坛,1,1");
		carIndustryMap.put("bbs.pcauto.com.cn","汽车论坛,1,1");
		carIndustryMap.put("baa.bitauto.com","汽车论坛,1,1");
		carIndustryMap.put("saa.auto.sohu.com","汽车论坛,1,1");
	}
	
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		TypeInfo tfin = TypeInfoUtils.getTypeInfoFromTypeString(inputSchema);
		inputOI = (StructObjectInspector) OrcStruct.createObjectInspector(tfin);
		setOtherDomainValue(context);
		setDomainValue();
		setKeyWordRule(context);
		setClassMap(context);
		setCarRule();
		setSpecialBv();
		setCarIndustryRule();
		setWebKeyWordConfig(context);
	}

	private String username = "";
	private String url = "";
	private String domain = "";
	private String sys = "";
	private String accesstime = "";
	private String referDomain = "";
	private String keyword="";

	protected void map(NullWritable meaningless, OrcStruct orc, Context context)
			throws IOException, InterruptedException {

		List<Object> ilst = inputOI.getStructFieldsDataAsList(orc);
		if (ilst.size() != 21)
			return;
		String[] items = new String[22];
		for (int i = 0; i < ilst.size(); i++) {
			items[i] = ilst.get(i).toString();
		}
		username = items[1];
		url = items[4];
		domain = items[3];
		accesstime = items[11];
		referDomain = items[15];
		keyword=items[13];
		if (items[6] != null)
			sys = items[6].toLowerCase().trim();
		paraseIndustry(context);
		countKeyWord(context);

	};
    private void countKeyWord(org.apache.hadoop.mapreduce.Mapper.Context context)throws IOException, InterruptedException{
    	if(keyword!=null&&keyword.length()>1){
	    	Map<String,String>  values=webmap.get(domain);
	    	  if(values!=null){
	    		  Set<Entry<String,String>>set=values.entrySet();
				   Iterator<Entry<String,String>> it=set.iterator();
			       while(it.hasNext()){
			    	   Entry<String,String>en=it.next();
			    	   if(keyword.contains(en.getKey())){
			    		   String keyvalus[]=en.getValue().split("@");
			    		   String keyid=keyvalus[1];
			    		   String attName=keyvalus[2];
			    		   String attValue=keyvalus[0];
			    		   String outkey = username + ","+attName+"," + attValue;//result2[5]
			    		   context.write(new Text("countDATA_webkey_"+keyid), new Text(1+"")); 
			    		   context.write(new Text("countDATA_webkeyLabel_"+outkey), new Text(1+"")); 
			    	   }
			    	   
			       }
				   
	    	  }
    	  }
    }
	private void paraseNewCar(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException{
		int flag = 2;
		if (sys.equals("windows") || sys.equals("macintosh")) {
			flag = 1;
		}

		String domains[] = domain.split("\\.");
		int domainLength = domains.length;
		if (domainLength > 1) {
			String domains2= domains[domainLength - 2];
			String rootDomain="";
			if("com".equals(domains2)){
			 rootDomain = domains[domainLength - 3]+"."+domains[domainLength - 2] + "."
					+ domains[domainLength - 1];
			 }else{
				 rootDomain =domains[domainLength - 2] + "."
							+ domains[domainLength - 1]; 
			 }
			String rules = carMap.get(rootDomain);
            if (referDomain.contains(rootDomain) || " ".equals(referDomain)) {
				referDomain = "1";
			}
            if (rules != null) {
				String[] result = paraseRules(rules, url, 3);
				String match = "";
				int jflag = Integer.parseInt(result[2]);
				if (jflag == 1) {
					match = result[3];
				}
				String outkey = username + "," + url + "," + match + ","
						+ result[0] + ",T1," + domain + "," + accesstime + ","
						+ flag + "," + referDomain + ",";
				if (!"".equals(match)) {
					context.write(new Text("newCar"), new Text(outkey));
					
					}
			}
		}
	}
	public void paraseType(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException{
    	
    	int flag = 2;
		if (sys.equals("windows") || sys.equals("macintosh")) {
			flag = 1;
		}
		String outkey=flag+","+username;
    	context.write(new Text("countDATA_type_" + outkey),
				new Text("1"));
    }
	public void paraseKeyWord(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {

		String items[] = getKeyUrl(context);
		if (items != null) {
			String url = items[0];
			String username = items[1];
			if (url.indexOf("http://shenzhen.anjuke.com/sale/") != -1) {
				outResult("shenzhen-anjuke-esf.dic", 1, 1, "-", url, context,
						username, kewordMap);
			} else if (url.indexOf("http://sz.fang.anjuke.com/loupan/") != -1) {
				outResult("shenzhen-anjuke-xf.dic", 1, 1, "_", url, context,
						username, kewordMap);
			} else if (url.indexOf("http://esf.sz.fang.com/") != -1) {
				outResult("shenzhen-fang-esf.dic", 0, 1, "-", url, context,
						username, kewordMap);
			} else if (url.indexOf("http://newhouse.sz.fang.com/house/s/") != -1) {
				outResult("shenzhen-fang-xf.dic", 2, 1, "-", url, context,
						username, kewordMap);
			} else if (url.indexOf("http://shenzhen.qfang.com/sale/") != -1) {
				outResult("shenzhen_qfang_esf.txt", 1, 1, "-", url, context,
						username, kewordMap);
			} else if (url.indexOf("http://shenzhen.qfang.com/newhouse/list") != -1) {
				outResult("shenzhen_qfang_xf.txt", 2, 1, "-", url, context,
						username, kewordMap);
			} else if (url.indexOf("http://sz.lianjia.com/ershoufang/") != -1) {
				outResult("shenzhen-lianjia-esf.dic", 1, 3, "-", url, context,
						username, kewordMap);
			} else if (url.indexOf("http://sz.fang.lianjia.com/loupan/") != -1) {
				outResult("shenzhen-lianjia-xf.dic", 1, 3, "-", url, context,
						username, kewordMap);
			}
		}
	}

	private void outResult(String pathname, int startpostion, int flag,
			String splitflag, String url,
			org.apache.hadoop.mapreduce.Mapper.Context context,
			String username, Map<String, String> kewordMap) throws IOException,
			InterruptedException {
		String paths[] = new URL(url).getPath().split("/");
		int pathlength = paths.length;
		String category = null;
		if (pathlength > 1) {
			for (int i = startpostion; i < pathlength; i++) {
				String path2[] = null;
				if (flag == 1) {
					if (!paths[i].contains("house"))
						path2 = paths[i].split(splitflag);
				}

				else if (flag == 3) {
					path2 = new String[4];
					Pattern p = Pattern
							.compile("nhtt[0-9]{1,2}|nht[0-9]{1}|p[0-9]{1}|l[0-9]{1}");
					Matcher m = p.matcher(paths[i]);
					int c = 0;
					while (m.find()) {
						path2[c] = m.group();
						c++;
						if (c >= 4)
							break;
					}

				}
				if (path2 != null && path2[0] != null && path2.length > 1) {
					for (int j = 0; j < path2.length; j++) {
						category = kewordMap.get(pathname + path2[j]);
						if (category != null)
							context.write(new Text("countDATA_keyword_"
									+ username + "," + category), new Text("1"));
					}

				} else {
					category = kewordMap.get(pathname + paths[i]);
					if (category != null)
						context.write(new Text("countDATA_keyword_" + username
								+ "," + category), new Text("1"));

				}
			}

		}

	}

	public String[] getKeyUrl(org.apache.hadoop.mapreduce.Mapper.Context context) {

		String domains[] = domain.split("\\.");
		int domainLength = domains.length;
		if (domainLength > 1) {
			String rootDomain = domains[domains.length - 2] + "."
					+ domains[domains.length - 1];
			String[] rules = getKwUrl(rootDomain, choosekeywordMap);
			boolean flag = true;
			if (rules != null) {
				try {
					if ("1".equals(rules[0])) {
						String paths[] = new URL(url).getPath().split("/");
						if (paths.length > 1) {
							rootDomain = rootDomain + "/" + paths[1];
						}
					} else {
						rootDomain = domains[0] + "." + rootDomain;
					}
					rules = getKwUrl(rootDomain, choosekeywordMap);
					flag = getKWurlFlag(rules, url);
				} catch (MalformedURLException e) {
					// TODO Auto-generated catch block
					// e.printStackTrace();
				}
			}
			if (!flag)
				url = "";

		}

		String[] result = { url, username };
		if ("".equals(url))
			result = null;
		return result;
	}

	private boolean getKWurlFlag(String[] rules, String url) {
		boolean flag = false;
		String regex = rules[0];
		if (!"1".equals(regex)) {
			Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
			Matcher m = p.matcher(url);
			if (m.find()) {
				flag = geturlRules2(rules[1], 1, url)
						&& geturlRules2(rules[2], 2, url);
			}
		} else {
			flag = geturlRules2(rules[1], 1, url)
					&& geturlRules2(rules[2], 2, url);
		}
		return flag;
	}

	private boolean geturlRules2(String rule, int yn, String url) {
		boolean flag = false;
		if (yn == 2)
			flag = true;
		String rules[] = rule.split("\\|");
		for (int i = 0; i < rules.length; i++) {
			if (yn == 1) {
				if (url.contains(rules[i]) || rules[i].equals("1"))
					flag = true;
			} else if (yn == 2) {
				if (url.contains(rules[i]) || rules[i].equals("1"))
					flag = false;
			}
		}
		return flag;
	}

	private String[] getKwUrl(String key, Map<String, String> map) {
		String value = map.get(key);
		String result[] = null;
		if (value != null) {
			result = value.split(",");
		}
		return result;
	}

	public void paraseIndustry(
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {

		StringBuilder sb = new StringBuilder();

		String match = classMap.get(domain);
		if (match != null) {
			String parentName = match.split("_")[0];
			String classaname = match.split("_")[1];
			String outkey = username + "," + parentName + "," + classaname;

			context.write(new Text("countDATA_industry_" + outkey), new Text(
					"1"));
		}
        
		try {
			paraseType(context);
			paraseNewHouse(context);// 解析新房数据
			paraseKeyWord(context);// 解析关键字信息
			paraseOtherHouse(context);// 解析房产行业标签
			paraseNewCar(context);
			paraseSpecialBv(context);// 解析汽车特定行为标签
			paraseCarIndustry(context);// 解析汽车行业标签
		} catch (Exception e) {
           
		}
	}

	public void paraseOtherHouse(
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {

		int flag = 2;
		if (sys.equals("windows") || sys.equals("macintosh")) {
			flag = 1;
		}

		String domains[] = domain.split("\\.");
		int domainLength = domains.length;
		if (domainLength > 1) {
			String rootDomain = domains[domains.length - 2] + "."
					+ domains[domains.length - 1];//组装根域名
			String rules = otherDomainMap.get(rootDomain);//获取此根源下第二级组装key的规则

			if (referDomain.contains(rootDomain) || " ".equals(referDomain)) {//判断溯源网站和访问网站是否是一个域名
				referDomain = "1";
			}
			boolean dflag = !domain.equals(rootDomain);
			String result2[]=null;
			if (rules != null) {
				String[] result = paraseRules(rules, url, 2);//解析url匹配规则
				String match = "";
				int jflag = Integer.parseInt(result[2]);
				if (jflag == 1) {
					match = result[3];
				} else if (jflag == 2 && dflag) {
					try {
						String paths[] = new URL(url).getPath().split("/");
						if (paths.length > 1) {
							rootDomain = rootDomain + "/" + paths[1];
							rules = otherDomainMap.get(rootDomain);
							if (rules != null) {
							 result2 = paraseRules(rules, url, 2);
								match = result2[3];
								if ("".equals(match)) {
									match = "其他";
								}
							}
						}
					} catch (MalformedURLException e) {
						// TODO Auto-generated catch block
						// e.printStackTrace();
					}
				} else if (jflag == 3 && dflag) {
					rootDomain = domain;
					rules = otherDomainMap.get(rootDomain);
					if (rules != null) {
						result2 = paraseRules(rules, url, 2);
						match = result2[3];
						if ("".equals(match)) {
							match = "其他";
						}
					}

				} else if (jflag == 4 && dflag) {
					try {
						String paths[] = new URL(url).getPath().split("/");
						if (paths.length > 2) {
							rootDomain = rootDomain + "/" + paths[1] + "/"
									+ paths[2];
							;
							rules = otherDomainMap.get(rootDomain);
							if (rules != null) {
								result2 = paraseRules(rules, url, 2);
								match = result2[3];
								if ("".equals(match)) {
									match = "其他";
								}
							}
						}
					} catch (MalformedURLException e) {
						// TODO Auto-generated catch block
						// e.printStackTrace();
					}
				} else if (jflag == 5 && dflag) {
					rootDomain = domain;
					rules = otherDomainMap.get(rootDomain);
					if (rules != null) {
						 result2 = paraseRules(rules, url, 2);
						match = result2[3];

						if ("".equals(match)) {
							match = "其他";
						}
					} else {

						String paths[] = new URL(url).getPath().split("/");
						if (paths.length > 1) {
							rootDomain = rootDomain + "/" + paths[1];
							;
							rules = otherDomainMap.get(rootDomain);
							if (rules != null) {
								 result2 = paraseRules(rules, url, 2);
								match = result2[3];
								if ("".equals(match)) {
									match = "其他";
								}
							}
						}
					}
				} else if (jflag == 6 && dflag) {
					try {
						String paths[] = new URL(url).getPath().split("/");
						if (paths.length > 2) {
							if (paths.length > 3) {
								rootDomain = rootDomain + "/" + paths[1] + "/"
										+ paths[2] + "/" + paths[3];
								rules = otherDomainMap.get(rootDomain);
								if (rules != null) {
									 result2 = paraseRules(rules, url,
											2);
									match = result2[3];
									if ("".equals(match)) {
										match = "其他";
									}
								}
							} else {
								rootDomain = rootDomain + "/" + paths[1] + "/"
										+ paths[2];
								;
								rules = otherDomainMap.get(rootDomain);
								if (rules != null) {
									 result2 = paraseRules(rules, url,
											2);
									match = result2[3];
									if ("".equals(match)) {
										match = "其他";
									}
								}
							}
						}
					} catch (MalformedURLException e) {
						// TODO Auto-generated catch block
						// e.printStackTrace();
					}
				}

				if (!"".equals(match)) {
				    match = match.substring(match.lastIndexOf("_") + 1);
					String outkey = username + "|" + url + "|" + match + "|"+result2[5]+"|"
							+ domain + "|" + accesstime + "|" + flag + "|"
							+ referDomain + "|";//
					// System.out.println(match);
					context.write(new Text("otherHouse"), new Text(outkey));
					outkey = username + ","+result2[5]+"," + match;//result2[5]
					context.write(new Text("countDATA_industry_" + outkey),
							new Text("1"));
					context.write(new Text("countDATA_lablefreq_" + result2[4]),
							new Text("1"));
				}
			}

		}

	}

	/*
	 * public static void main(String args[]){ String line=
	 * "44300|192.168.3.239|192.168.3.239|sz.fang.anjuke.com|http://sz.fang.anjuke.com/loupan/nanshan/d19_w1|sz.fang.anjuke.com/loupan/all/d19_w1/,windows|NT 6.1|chrome|33.0.1750.146| |20160701093851| | |0|sz.fang.anjuke.com|0"
	 * ; try { new AnalysisDataLabelMapper().paraseKeyWord(line, null); } catch
	 * (Exception e) { // TODO Auto-generated catch block e.printStackTrace(); }
	 * }
	 */
	public void paraseNewHouse(
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {

		int flag = 2;
		if (sys.equals("windows") || sys.equals("macintosh")) {
			flag = 1;
		}

		String domains[] = domain.split("\\.");
		int domainLength = domains.length;
		if (domainLength > 1) {
			String rootDomain = domains[domainLength - 2] + "."
					+ domains[domainLength - 1];
			String rules = domainMap.get(rootDomain);

			if (referDomain.contains(rootDomain) || " ".equals(referDomain)) {
				referDomain = "1";
			}
			if (rules != null) {
				String[] result = paraseRules(rules, url, 1);
				String match = "";
				int jflag = Integer.parseInt(result[2]);
				if (jflag == 1) {
					match = result[3];
				} else if (jflag == 2) {

					String paths[] = new URL(url).getPath().split("/");
					if (paths.length > 1) {
						rootDomain = rootDomain + "/" + paths[1];
						rules = domainMap.get(rootDomain);
						if (rules != null) {
							String result2[] = paraseRules(rules, url, 2);
							match = result2[3];
							if ("".equals(match)) {
								match = result[3];
							}
						} else {
							match = result[3];
						}
					} else {
						match = result[3];
					}

				}

				String outkey = username + "," + url + "," + match + ","
						+ result[0] + ",T1," + domain + "," + accesstime + ","
						+ flag + "," + referDomain + ",";
				if (!"".equals(match)) {
					context.write(new Text("newHouse"), new Text(outkey));
					outkey = username + ",房产,新房";
					context.write(new Text("countDATA_industry_" + outkey),
							new Text("1"));
				}
			}
		}

	}

	public void paraseSpecialBv(
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {

		int flag = 2;
		if (sys.equals("windows") || sys.equals("macintosh")) {
			flag = 1;
		}

		String domains[] = domain.split("\\.");
		int domainLength = domains.length;
		if (domainLength > 1) {
			String rootDomain = domains[domainLength - 2] + "."
					+ domains[domainLength - 1];
			if (referDomain.contains(rootDomain) || " ".equals(referDomain)) {
				referDomain = "1";
			}
		
			String[] types={"对比","试驾","询价","注册","登录"};
			for(String type:types){
				String rules = specialbvMap.get(domain+"_"+type);
				if(StringUtils.isNotEmpty(rules)){
					String[] splits=rules.split(",");
					String resultValue=findMatho(url, splits[0],Integer.parseInt(splits[1]));
					if(StringUtils.isNotEmpty(resultValue)){
						String outkey = username + "|" + url + "|" + type
								+ "|特定行为|" + domain + "|" + accesstime + "|"
								+ flag + "|" + referDomain + "|";
						context.write(new Text("specialbv"), new Text(outkey));
						outkey = username + ",特定行为,"+type;
						context.write(new Text("countDATA_specialbvct_" + outkey),
								new Text("1"));
					}
				}
			}
		}
	}
	
	public void paraseCarIndustry(
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {

		int flag = 2;
		if (sys.equals("windows") || sys.equals("macintosh")) {
			flag = 1;
		}

		String domains[] = domain.split("\\.");
		int domainLength = domains.length;
		if (domainLength > 1) {
			String rootDomain = domains[domains.length - 2] + "."
					+ domains[domains.length - 1];
			if (referDomain.contains(rootDomain) || " ".equals(referDomain)) {
				referDomain = "1";
			}
			boolean dflag = !domain.equals(rootDomain);
			
			String match = "";
			String rules = carIndustryMap.get(domain);
			String mhrules = mohucarIndustryMap.get(rootDomain);
			
			if(StringUtils.isNotEmpty(rules)){
				String[] result = paraseRules(rules, url);
				match=result[3];
			}else if(StringUtils.isNotEmpty(mhrules)){
				String[] result = paraseRules(mhrules, url);
				match=result[3];
			}
			
			if(StringUtils.isNotEmpty(match)){
				String outkey = username + "|" + url + "|" + match
						+ "|汽车|" + domain + "|" + accesstime + "|"
						+ flag + "|" + referDomain + "|";
				context.write(new Text("carindustry"), new Text(outkey));
				outkey = username + ",汽车," + match;
				context.write(new Text("countDATA_industry_" + outkey),
						new Text("1"));
			}
		}
	}

	
	private String[] paraseRules(String rules, String url, int flag) {
		String[] result = new String[6];
		String []trules=rules.split(",");
		String source = trules[0];
		String regex = trules[1];
		String jflag = trules[2];
		result[0] = source;
		result[1] = regex;
		result[2] = jflag + "";
        if(trules.length==6){
        	result[4]=trules[4];
        	result[5]=trules[5];
        }
		String match = "";
		if (flag != 2  || (flag == 2 && "1".equals(jflag))) {
			if (regex.contains("|")) {
				String regexs[] = regex.split("\\|");
				for (int i = 0; i < regexs.length; i++) {
					match = findMathc(url, regexs[i]);
					if (!"".equals(match)) {
						if(flag==3)
							match = source + "_" + match;
						else
						match = match + "_" + source;
						break;
					}
				}
			} else {
				match = findMathc(url, regex);
				if (!"".equals(match)) {
					if(flag==3)
						match = source + "_" + match;
					else
					match = match + "_" + source;

				}

			}
		}
		result[3] = match;
		return result;
	}

	private String findMathc(String url, String regex) {
		Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(url);
		String match = "";

		while (m.find()) {
			match = m.group(1);
		}
		return match;
	}
	
	private String findMatho(String str, String regEx, int group) {
		
		String resultValue = null;
 		if ((str == null) || (regEx == null) || ((regEx != null) && ("".equals(regEx.trim())))) 
 			return resultValue;
 		
 		
 		Pattern p = Pattern.compile(regEx);
 		Matcher m = p.matcher(str);

 		boolean result = m.find();
 		if (result)
 		{
 			resultValue = m.group(group);
 		}
 		return resultValue;
	}
	
	 private  String[] paraseRules(String rules, String url) {
			String[] res = new String[4];
			String name = rules.split(",")[0];
			String regex = rules.split(",")[1];
			String flag = rules.split(",")[2];
			res[0] = name;
			res[1] = regex;
			res[2] = flag;

			String result = "";
			if (!"1".equals(flag)) {
				    result = findMathc(url, res[1]);
					if (!"".equals(result)) {
						result = res[0];
				}
			}else{
				result = res[0];
			}
			res[3] = result;
			return res;
		}
	
}

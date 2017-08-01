package com.aotain.project.sada;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;




import com.aotain.common.CommonFunction;

public class AnalysisDataLabelMapper extends
		Mapper<LongWritable,Text,Text,Text> {
	public Map<String, String> otherDomainMap = new HashMap<String, String>();
	public Map<String, String> domainMap = new HashMap<String, String>();
	public Map<String, String> kewordMap = new HashMap<String, String>();
	HashMap<String, String> classMap = new HashMap<String, String>();
	Map<String, String> choosekeywordMap = new HashMap<String, String>();
	public Map<String,String>  carMap=new HashMap<String, String>();
	public Map<String,String>  specialbvMap=new HashMap<String, String>();
	public Map<String, String> carIndustryMap = new HashMap<String, String>();
	public Map<String, String> mohucarIndustryMap = new HashMap<String, String>();
	public Map<String,String>ershouseMap=new HashMap<String, String>();
	public Map<String,String>otherLabelMap=new HashMap<String, String>();
	public Map<String,String>matchMap=new HashMap<String, String>();
	public    Map<String,String>matchlabelValue=new HashMap<String, String>();
	public String kWs="";
    public String kwc;
    public void setJXQYiNFO(Context context) throws IOException{
    	String confuri = context.getConfiguration().get("kwPath");
		Configuration config = context.getConfiguration();
        FileSystem fs = FileSystem.get(URI.create(confuri), config);
		FSDataInputStream in = null;
		Path path = new Path(confuri);
		if (fs.exists(path)) {
			System.out.println(confuri);
			System.out.println("exist conf file !!!!!!");
			StringBuffer kwstr = new StringBuffer();
			try {
				if (fs.isDirectory(path)) {
					for (FileStatus file : fs.listStatus(path)) {
					
					
						try {
							// file.readFields(in);
							in = fs.open(file.getPath());
							BufferedReader bis = new BufferedReader(
									new InputStreamReader(in, "UTF8"));
							String line = "";
							while ((line = bis.readLine()) != null) {
								kwstr.append(line.trim()).append("@@@");
								}
								// IP,ADSL
							}
							// deviceList.deleteCharAt(deviceList.length()-1);//ȥ�����һ������
							// System.out.println(deviceList.toString());

						 finally {
							IOUtils.closeStream(in);
						}
					}
				}
				String kW = kwstr.toString();
				kWs=kW.substring(0, kW.length()-1);
			} finally {
				IOUtils.closeStream(in);
			}
		} else {
		
			System.out.println("not exist file !!!!!!");
		}
    }
    public void setOtherLabel(){
    	//有小孩
    	otherLabelMap.put("fileyun1.eachbaby.com", "家庭组织,有小孩2");
    	otherLabelMap.put("video.quarkedu.com", "家庭组织,有小孩2");
    	otherLabelMap.put("api.bbpapp.com", "家庭组织,有小孩2");
    	otherLabelMap.put("toylite.luckygz.com", "家庭组织,有小孩2");
    	otherLabelMap.put("appweb.babaxiong.com", "家庭组织,有小孩2");
    	otherLabelMap.put("fileyun1.eachbaby.com", "家庭组织,有小孩2");
    	otherLabelMap.put("babyvideo.kssws.ks-cdn.com", "家庭组织,有小孩2");
    	otherLabelMap.put("7ximqs.com2.z0.glb.qiniucdn.com", "家庭组织,有小孩2");
    	otherLabelMap.put("atm.punchbox.org", "家庭组织,有小孩2");
    	otherLabelMap.put("app-zh.babybus.org", "家庭组织,有小孩2");
    	otherLabelMap.put("api.ibbpp.com", "家庭组织,有小孩2");
    	otherLabelMap.put("app-zh.babybus.org", "家庭组织,有小孩2");
    	otherLabelMap.put("s2.cdn.xiachufang.com", "家庭组织,有小孩2");
    	otherLabelMap.put("papi.mama.cn", "家庭组织,有小孩2");
    	otherLabelMap.put("s08.lmbang.com", "家庭组织,有小孩2");
    	otherLabelMap.put("s05.lmbang.com", "家庭组织,有小孩2");
    	otherLabelMap.put("s03.lmbang.com", "家庭组织,有小孩2");
    	otherLabelMap.put("open.lmbang.com", "家庭组织,有小孩2");
    	otherLabelMap.put("www.ladybirdedu.com", "家庭组织,有小孩2");
    	otherLabelMap.put("fileyun1.eachbaby.com ", "家庭组织,有小孩2");
    	otherLabelMap.put("api.babytree.com", "家庭组织,有小孩2");
    	otherLabelMap.put("peanut.zhilehuo.com", "家庭组织,有小孩2");
    	otherLabelMap.put("mobads-logs.baidu.com", "家庭组织,有小孩2");
    	otherLabelMap.put("sc.seeyouyima.com", "家庭组织,有小孩2");
    	otherLabelMap.put("mobads-logs.baidu.com", "家庭组织,有小孩2");
        //高端金融
    	otherLabelMap.put("www.idgvc.com","金融,高端金融");
    	    	otherLabelMap.put("www.sbcvc.com","金融,高端金融");
    	    	otherLabelMap.put("www.sequoiacap.com","金融,高端金融");
    	    	otherLabelMap.put("www.gs.com","金融,高端金融");
    	    	otherLabelMap.put("www.morganstanley.com","金融,高端金融");
    	    	otherLabelMap.put("www.jinfuzi.com","金融,高端金融");
    	    	otherLabelMap.put("simu.howbuy.com","金融,高端金融");
    	    	otherLabelMap.put("dc.simuwang.com","金融,高端金融");
    	    	otherLabelMap.put("www.touzi.com","金融,高端金融");
    	    	otherLabelMap.put("www.jrjr.hk","金融,高端金融");
    	    	otherLabelMap.put("www.mibd-ecn.com","金融,高端金融");
    	    	otherLabelMap.put("www.firstbullion.com","金融,高端金融");
    	    	otherLabelMap.put("www.igoldhk.com","金融,高端金融");
    	    	otherLabelMap.put("www.24k.hk","金融,高端金融");
    	    	otherLabelMap.put("www.yumaoyi.com","金融,高端金融");
    	    	otherLabelMap.put("www.jaadee.com","金融,高端金融");
    	    	otherLabelMap.put("www.mk169.com","金融,高端金融");
    	    	otherLabelMap.put("www.neeq.com.cn","金融,高端金融");
    	   //l留学
    	    	otherLabelMap.put("vip.jjl.cn","教育情况,出国留学");
    	    	otherLabelMap.put("www.gasheng-edu.com","教育情况,出国留学");
    	    	otherLabelMap.put("www.eic.org.cn","教育情况,出国留学");
    	    	otherLabelMap.put("www.zmnedu.com","教育情况,出国留学");
    	    	otherLabelMap.put("sub.apexedugroup.com","教育情况,出国留学");
    	    	otherLabelMap.put("www.aoji.cn","教育情况,出国留学");
    	    	otherLabelMap.put("tiandaoedu.com","教育情况,出国留学");
    	    	otherLabelMap.put("shenzhen.gedu.org","教育情况,出国留学");
    	    	otherLabelMap.put("www.educhuguo.cn","教育情况,出国留学");
    	    	otherLabelMap.put("www.eickaopei.com","教育情况,出国留学");
    	    	otherLabelMap.put("sz.xdf.cn","教育情况,出国留学");
    	    	otherLabelMap.put("www.meten.cn","教育情况,出国留学");
    	    	otherLabelMap.put("www.risecenter.com","教育情况,出国留学");
    	    	otherLabelMap.put("www.webienglish.cn","教育情况,出国留学");
    	    	otherLabelMap.put("www.ef.com.cn","教育情况,出国留学");
    	    	otherLabelMap.put("www.eblockschina.com","教育情况,出国留学");
    	    	otherLabelMap.put("www.englishbreak.cn","教育情况,出国留学");
    	    	otherLabelMap.put("sz.jingrui.cn","教育情况,出国留学");
    	    	otherLabelMap.put("www.zgjhjy.com","教育情况,出国留学");
    	    	otherLabelMap.put("hf100.net","教育情况,出国留学");
    	    	otherLabelMap.put("www.xdjiaoyu.cn","教育情况,出国留学");
    	    	otherLabelMap.put("sz.jiajiaoban.com","教育情况,出国留学");
    	    	otherLabelMap.put("www.worldwayhk.com","教育情况,出国留学");
    	    	otherLabelMap.put("www.imchinese.net","教育情况,出国留学");
    	    	otherLabelMap.put("www.yiminjiayuan.com","教育情况,出国留学");
    	    	otherLabelMap.put("www.heyvisa.com","教育情况,出国留学");
    	    	otherLabelMap.put("www.globevisa.com.cn","教育情况,出国留学");
    	    	otherLabelMap.put("lx.welltrend.com.cn","教育情况,出国留学");
    	    	otherLabelMap.put("www.aoji.cn","教育情况,出国留学");
    	    	otherLabelMap.put("www.ekimmigration.com","教育情况,出国留学");
    	    	otherLabelMap.put("www.gasheng.com","教育情况,出国留学");
    	    	otherLabelMap.put("www.ivygate.com.cn","教育情况,出国留学");
    	    	otherLabelMap.put("www.wailianvisa.com","教育情况,出国留学");
    	    	otherLabelMap.put("www.ushighschools.net","教育情况,出国留学");
    	    	otherLabelMap.put("www.findingschool.net","教育情况,出国留学");
    	    	otherLabelMap.put("www.privateschoolreview.com","教育情况,出国留学");
    	    	otherLabelMap.put("www.boardingschoolreview.com","教育情况,出国留学");
    	    	otherLabelMap.put("www.greatschools.org","教育情况,出国留学");
    	    			//旅游
    	    	otherLabelMap.put("vacations.ctrip.co","旅游,知名旅游");
    	    	otherLabelMap.put("www.tuniu.com","旅游,知名旅游");
    	    	otherLabelMap.put("ihotel.elong.com","旅游,知名旅游");
    	    	otherLabelMap.put("trip.elong.com","旅游,知名旅游");
    	    	otherLabelMap.put("www.ly.com","旅游,知名旅游");
    	    	otherLabelMap.put("zt.dujia.qunar.com","旅游,知名旅游");
    	    	otherLabelMap.put("m.tuniu.com","旅游,知名旅游");
    	    	otherLabelMap.put("mobile-api2011.elong.com","旅游,知名旅游");
    	    	otherLabelMap.put("www.findingschool.net","旅游,知名旅游");
    	    	otherLabelMap.put("www.privateschoolreview.com","旅游,知名旅游");
    	    	otherLabelMap.put("www.boardingschoolreview.com","旅游,知名旅游");
    	    	otherLabelMap.put("www.greatschools.org","旅游,知名旅游");
    	    	//房产
    	    	otherLabelMap.put("appservice.jia.com","房产app,齐家");
    	    	otherLabelMap.put("esf.leju.com","房产app,乐居");
    	    	otherLabelMap.put("api.zhugefang.com","房产app,诸葛找房");
    	    	otherLabelMap.put("fs.esf.fangdd.com","房产app,房多多");
    	    	otherLabelMap.put("img.agjimg.com","房产app,安个家房产");
    	    	otherLabelMap.put("sh.fang.lianjia.com","房产app,链家（上海）");
    	    	otherLabelMap.put("ta.2boss.cn","房产app,兔博士");
    	    	otherLabelMap.put("www.ftoutiao.com","房产app,房头条");
    	    	otherLabelMap.put("app.esf.sina.com.cn","房产app,新浪二手房");
    	    	otherLabelMap.put("shanghai.qfang.com","房产app,q房");
    	    	otherLabelMap.put("m.fangdd.com","房产app,房多多手机版");
    	    	//淘宝卖家
    	    	otherLabelMap.put("zhitongche.taobao.com",  "购物,淘宝天猫卖家");
    	    	otherLabelMap.put("ad.alimama.com",         "购物,淘宝天猫卖家");
    	    	otherLabelMap.put("fuwu.taobao.com",        "购物,淘宝天猫卖家");
    	    	otherLabelMap.put("shu.taobao.com",         "购物,淘宝天猫卖家");
    	    	otherLabelMap.put("zuanshi.taobao.com",     "购物,淘宝天猫卖家");
    	    	otherLabelMap.put("shuyuan.taobao.com",     "购物,淘宝天猫卖家");
    	    	otherLabelMap.put("mofang.taobao.com",      "购物,淘宝天猫卖家");
    	    	otherLabelMap.put("qianniu.fuwu.taobao.com","购物,淘宝天猫卖家");
    	    	otherLabelMap.put("daxue.taobao.com",       "购物,淘宝天猫卖家");
    	    	otherLabelMap.put("www.alimama.com",        "购物,淘宝天猫卖家");
    	    	otherLabelMap.put("pub.alimama.com",        "购物,淘宝天猫卖家");
    	    	otherLabelMap.put("www.moojing.com",        "购物,淘宝天猫卖家");
    	    	otherLabelMap.put("club.alimama.com",       "购物,淘宝天猫卖家");
    	    	otherLabelMap.put("www.iwshang.com",        "购物,淘宝天猫卖家");
    	    	otherLabelMap.put("myseller.taobao.com",    "购物,淘宝天猫卖家");
    	    	otherLabelMap.put("beta.sycm.taobao.com",   "购物,淘宝天猫卖家");
    	    	otherLabelMap.put("subway.simba.taobao.com","购物,淘宝天猫卖家");
    	    	otherLabelMap.put("ad.alimama.com",         "购物,淘宝天猫卖家");
    	    	otherLabelMap.put("sycm.taobao.com",        "购物,淘宝天猫卖家");
    	    	otherLabelMap.put("branding.taobao.com",    "购物,淘宝天猫卖家");
    	    	otherLabelMap.put("majibao.alimama.com",    "购物,淘宝天猫卖家");
    	    	otherLabelMap.put("www.wshang.com",         "购物,淘宝天猫卖家");
    	    	otherLabelMap.put("meizhe.meideng.net",     "购物,淘宝天猫卖家");
    	    	otherLabelMap.put("ex37.kuaidizs.com",      "购物,淘宝天猫卖家");
    	    	otherLabelMap.put("f.superboss.cc",         "购物,淘宝天猫卖家");
    	    	otherLabelMap.put("cdn.zzgdapp.com",        "购物,淘宝天猫卖家");
    	    	otherLabelMap.put("tao21.org",              "购物,淘宝天猫卖家");
    	    	otherLabelMap.put("puyunsoft.com",          "购物,淘宝天猫卖家");

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
		String confuri = context.getConfiguration().get("Classconfig");
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
    private void setMatchRule(){
 
  	    matchlabelValue.put("com.anjuke.android.app", "APP需求者,安居客");
  	    matchlabelValue.put("com.soufun.app", "APP需求者,房天下");
  	    matchlabelValue.put("com.homelink.android", "APP需求者,链家");
  	    matchlabelValue.put("com.fangdd.mobile.fddhouseownersell", "APP需求者,房多多");
  	    matchlabelValue.put("com.lianjia.sh.android", "APP需求者,链家上海");
  	    matchlabelValue.put("com.leju.platform", "APP需求者,乐居");
  	    matchlabelValue.put("com.sohu.focus.apartment", "APP需求者,搜狐购房");
  	    matchlabelValue.put("com.ganji.android", "APP需求者,赶集网");
  	    matchlabelValue.put("com.android.daikuan.api", "APP需求者,房贷计算器");
  	    matchlabelValue.put("com.wuba", "APP需求者,58同城");
  	    matchlabelValue.put("com.qijia.o2o", "APP需求者,齐家");
  	    matchlabelValue.put("com.zhugezhaofang", "APP需求者,诸葛找房");
  	    matchlabelValue.put("com.angejia.android.app", "APP需求者,安个家房产");
  	    matchlabelValue.put("com.rabbit.doctor", "APP需求者,兔博士");
  	    matchlabelValue.put("com.android.qfangpalm", "APP需求者,Q房网");
  	    matchlabelValue.put("cn.com.sina_esf", "APP需求者,新浪二手房");
  	    matchlabelValue.put("cn.com.sina_esf", "APP需求者,新浪二手房");
  	    matchlabelValue.put("166730", "APP需求者,链家");
  	    matchlabelValue.put("150391", "APP需求者,安居客");
  	    matchlabelValue.put("150521", "APP需求者,房天下");
  	    matchlabelValue.put("266101", "APP需求者,q房网");
  	    matchlabelValue.put("226816", "APP需求者,齐家");
  	    matchlabelValue.put("268180", "APP需求者,安个房房产");
  	    matchlabelValue.put("266049", "APP需求者,上海链家");
	    matchlabelValue.put("145690", "APP需求者,新房贷计算器详情");
	    matchlabelValue.put("145274", "APP需求者,房贷计算器详情");
	    matchlabelValue.put("053ee4028e93c842827a019a950610298af414a6b", "APP需求者,链家");
	    matchlabelValue.put("045cc58089ff91aec329ad559f7018178284128fa", "APP需求者,安居客");
	    matchlabelValue.put("0fa5356892522a759213f5e80be38dd14fb40d5ca", "APP需求者,齐家");
	    matchlabelValue.put("0bc89b4986368497638a2298f43b0771df098688b ", "APP需求者,乐居");
	    matchlabelValue.put("06d5d439e7a46b33c19aefd09e428b5c73843bcff", "APP需求者,诸葛找房");
	    matchlabelValue.put("011d1740fe33f44542b17726182f8fb6f5f37a6f5", "APP需求者,房多多");	   
	    matchlabelValue.put("014c353bcc0dc09950c0db4522a4241758c41db37", "APP需求者,房贷计算器");
	    matchlabelValue.put("0a300756be493457d2ffaaa0d092ce23f0967c110", "APP需求者,房天下");
	    matchlabelValue.put("0ab4994ddec724ae307041c728a0b2f2c517e1e34", "APP需求者,安个家房产");
	    matchlabelValue.put("0e567545cc5f24f462197279b633a728d804cd86d", "APP需求者,兔博士");
	    matchlabelValue.put("0c807576f64d5d5b57dd0e1e72179c1fce84075e2", "APP需求者,Q房网");
	    matchlabelValue.put("0c31a451a46b26d09e6b65686a58b9bc866427d6f", "APP需求者,新浪二手房");
	    matchlabelValue.put("icon_33563_1479455494", "APP需求者,链家");
	    matchlabelValue.put("icon_6694_1479464358", "APP需求者,安居客");
	    matchlabelValue.put("icon_10912344_1478501233", "APP需求者,齐家");
	    matchlabelValue.put("icon_12208139_1479984013", "APP需求者,诸葛找房");
	    matchlabelValue.put("icon_10491262_1479808142", "APP需求者,房多多");	   
	    matchlabelValue.put("icon_1197183_1478156563", "APP需求者,房天下");
	    matchlabelValue.put("icon_11992558_1479694999", "APP需求者,安个家房产");
	    matchlabelValue.put("icon_42355421_1479866330", "APP需求者,兔博士");
	    matchlabelValue.put("icon_10587198_1479885138", "APP需求者,Q房网");
	    matchlabelValue.put("icon_12132560_1479204321", "APP需求者,新浪二手房");

	    matchMap.put("recommend.api.sj.360.cn", "http://recommend.api.sj.360.cn/mintf/getRecommandAppsForDetail?pname=([a-z\\.]+)&");
    	matchMap.put("ios3.app.i4.cn", "http://ios3.app.i4.cn/appinfo.xhtml\\?appid=([0-9]+)&");
    	matchMap.put("apis.wandoujia.com", "http://apis.wandoujia.com/five/v2/apps/([a-z\\.]+)\\?");
    	matchMap.put("t1.market.xiaomi.com", "http://t1.market.xiaomi.com/thumbnail/webp/l168q80/AppStore/([a-z0-9]+)$");
    	matchMap.put("pp.myapp.com:80", "http://pp.myapp.com:80/ma_icon/0/([a-z_0-9]+)/256");
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
		otherDomainMap
				.put("58.com",
						"二手房,(http://.*\\.58\\.com/ershoufang/[0-9]+x\\.shtml)\\??|(http://.*\\.58\\.com/[a-z]*/?ershoufang/?[0-9a-z]*)\\??,1");
		otherDomainMap.put("ifeng.com", "新房,1,2");
		otherDomainMap
				.put("ifeng.com/sale",
						"新房,(http://.*\\.house\\.ifeng\\.com/sale/search/36688/_/_/[0-9_]+\\.shtml)\\??,1");
		otherDomainMap.put("ifeng.com/homedetail",
				"新房,(http://.*\\.house\\.ifeng\\.com/homedetail/[0-9]+)\\??,1");
		otherDomainMap
				.put("ifeng.com/world",
						"海外房产,(http://world\\.house\\.ifeng\\.com/world/[A-Z]+/_/_/[0-9_]+\\.shtml)\\??|(http://world\\.house\\.ifeng\\.com/world/homedetail/[0-9]+)\\??,1");
		otherDomainMap
				.put("ifeng.com/detail",
						"海外房产,(http://world\\.house\\.ifeng\\.com/detail/[0-9_]+/[0-9_]+\\.shtml)\\??|(http://.*\\.house\\.ifeng\\.com/detail/[0-9_]+/[0-9_]+\\.shtml)\\??,1");

		otherDomainMap.put("centanet.com", "新房,1,2");
		otherDomainMap
				.put("centanet.com/xinfang",
						"新房,(http://.*\\.centanet\\.com/xinfang/lp-[0-9A-Z]+/?)\\??|(http://.*\\.centanet\\.com/xinfang/[a-z]*/?[0-9a-z]*/?)\\??,1");
		otherDomainMap
				.put("centanet.com/ershoufang",
						"二手房,(http://.*\\.centanet\\.com/ershoufang/[-0-9a-z]+\\.html)\\??|(http://.*\\.centanet\\.com/ershoufang/[a-z]*/?[0-9a-z]*/?)\\??,1");
		otherDomainMap
				.put("centanet.com/xzl",
						"新房,(http://.*\\.centanet\\.com/xzl/BusinessShop/Search/[-_%0-9a-zA-Z]+)\\??|(http://.*\\.centanet\\.com/xzl/BusinessShop/[dD]+etails/[-0-9a-z]+)\\??,1");
		otherDomainMap
				.put("centanet.com/xiaoqu",
						"二手房,(http://.*\\.centanet\\.com/xiaoqu/[a-z]*/?[0-9a-z]*/?)\\??|(http://.*\\.centanet\\.com/xiaoqu/[a-z]+-[a-z]+/?)\\??,1");
		otherDomainMap.put("qq.com", "新房,1,3");
		otherDomainMap.put("db.house.qq.com",
				"新房,(http://db\\.house\\.qq\\.com/.*_[0-9]+/?)\\??,1");
		otherDomainMap
				.put("esf.db.house.qq.com",
						"二手房,(http://esf\\.db\\.house\\.qq\\.com/.*/sale/[0-9]+_[0-9]+/?)\\??,1");
		otherDomainMap.put("ganji.com", "新房,1,2");
		otherDomainMap
				.put("ganji.com/fang5",
						"新房,(http://.*\\.ganji\\.com/fang5/[0-9]+x\\.htm)\\??|(http://.*\\.ganji\\.com/fang5/[a-z]*/?[a-z0-9]*/?)\\??,1");
		otherDomainMap.put("163.com", "新房,1,3");
		otherDomainMap
				.put("xf.house.163.com",
						"新房,(http://xf\\.house\\.163.com/sz/search/[0-9-]+\\.html)\\??|(http://xf\\.house\\.163.com/sz/[A-Z0-9]+\\.html)#?,1");
		otherDomainMap
				.put("vanke.com",
						"新房,(http://life\\.vanke\\.com/web/goodsDetail\\?GoodsID=[0-9]+&GoodsType=[0-9]+)&?,1");
		otherDomainMap.put("jjshome.com", "新房,1,2");
		otherDomainMap
				.put("jjshome.com/ysl",
						"新房,(http://.*\\.jjshome\\.com/ysl/[0-9]+)\\??|(http://.*\\.jjshome\\.com/ysl/index/[0-9a-z]+)\\??,1");
		otherDomainMap
				.put("jjshome.com/esf",
						"二手房,(http://.*\\.jjshome\\.com/esf/[0-9]+)\\??|(http://.*\\.jjshome\\.com/esf/index/[0-9a-z]+)\\??,1");
		otherDomainMap.put("fangdd.com", "新房,1,3");

		otherDomainMap.put("m.fangdd.com", "新房,(http://m.fangdd.com/nh),1");
		otherDomainMap
				.put("hd.xf.fangdd.com", "新房,(http://hd.xf.fangdd.com),1");
		otherDomainMap.put("fs.esf.fangdd.com",
				"二手房,(http://fs.esf.fangdd.com),1");
		otherDomainMap
				.put("xf.fangdd.com",
						"新房,(http://xf\\.fangdd\\.com/[a-z]+/[0-9]+\\.html)\\??|(http://xf\\.fangdd\\.com/.*/loupan/[-0-9a-z]+)\\??,1");
		otherDomainMap
				.put("esf.fangdd.com",
						"二手房,(http://esf\\.fangdd\\.com/[a-z]+/[0-9]+\\.html)\\??|(http://esf\\.fangdd\\.com/shenzhen/list/[-_%A-Z0-9a-z]+)\\??,1");
		otherDomainMap.put("baidu.com", "新房,1,3");
		otherDomainMap
				.put("fang.baidu.com",
						"新房,(http://fang\\.baidu\\.com/secondhouse\\?uid=[0-9]+&uid_type=house&estate_id=[0-9]+&city_id=340)\\??,1");
		otherDomainMap.put("szhome.com", "新房,1,2");
		otherDomainMap.put("szhome.com/house",
				"新房,(http://m\\.leju\\.com/touch/house/sz/[0-9]+/?)\\??,1");
		otherDomainMap.put("szhome.com/sell",
				"二手房,(http://esf\\.szhome\\.com/sell/[0-9]+\\.html)\\??,1");
		otherDomainMap.put("leju.com", "新房,1,4");
		otherDomainMap.put("leju.com/touch/house",
				"新房,(http://m\\.leju\\.com/touch/house/sz/[0-9]+/?)\\??,1");
		otherDomainMap
				.put("leju.com/touch/esf",
						"二手房,(http://m\\.leju\\.com/touch/esf/sz/detail/[0-9]+)\\??|(http://m\\.leju\\.com/touch/esf/sz/detail/[-0-9a-z]+/?)\\??,1");
		otherDomainMap.put("anjuke.com", "新房,1,5");
		otherDomainMap.put("api.anjuke.com",
				"新房,(http://api.anjuke.com/mobile/v5/recommend/sale),1");
		otherDomainMap.put("m.anjuke.com",
				"海外房产,(http://m.anjuke.com/haiwai),1");
		otherDomainMap
				.put("s.anjuke.com",
						"海外房产,(http://s.anjuke.com/st?__site=m_anjuke&p=Haiwai)|(http://s.anjuke.com/st?__site=m_anjuke-npv&p=Haiwai),1");
		otherDomainMap
				.put("api.fang.anjuke.com",
						"新房,(http://api.fang.anjuke.com/m/android/1.3/loupan/newlistv2)|(http://api.fang.anjuke.com/m/android/1.3/loupan/Newimages)|(http://api.fang.anjuke.com/m/android/1.3/loupan/lessGussLike),5");
		otherDomainMap
				.put("anjuke.com/haiwai",
						"海外房产,(http://[a-z]+\\.haiwai\\.anjuke\\.com/detail/building-[0-9]+\\.html)\\??,1");
		otherDomainMap.put("fang.com", "新房,1,5");
		otherDomainMap
				.put("fang.com/house",
						"新房,(http://[0-9a-z]+\\.fang\\.com/house/[0-9_]+\\.htm)\\??|(http://[a-z]+\\.fang\\.com/house/[0-9]+\\.htm)\\??,1");
		otherDomainMap.put("shop.fang.com",
				"商业地产,(http://shop\\.fang\\.com/shou/[0-9_]+\\.html)\\??,1");
		otherDomainMap.put("office.fang.com",
				"商业地产,(http://office\\.fang\\.com/zu/[0-9_]+\\.html)\\??,1");
		otherDomainMap
				.put("m.fang.com",
						"新房,(http://m.fang.com/world)|(http://m.fang.com/fangjia/?c=pinggu&a=worldFangjia&src=client]),1");
		otherDomainMap
				.put("soufunapp.3g.fang.com",
						"新房,(http://soufunapp.3g.fang.com/http/sf2014.jsp?city=%E6%B7%B1%E5%9C%B3&location=newhouse)|(http://soufunapp.3g.fang.com/http/sfservice.jsp?channel=houselist&city=%E6%B7%B1%E5%9C%B3&housetype=xf)|(http://soufunapp.3g.fang.com/http/sfservice.jsp?city=%E6%B7%B1%E5%9C%B3&fromType=1&messagename=getNewHousePriceData),1");
		otherDomainMap.put("soufunappxf.3g.fang.com",
				"新房,(http://soufunappxf.3g.fang.com),1");
		otherDomainMap.put("lianjia.com", "新房,1,5");
		otherDomainMap
				.put("lianjia.com/house",
						"海外房产,(http://[a-z]+\\.lianjia\\.com/house/[A-Z0-9]+\\.html)\\??,1");
		otherDomainMap
				.put("image2.lianjia.com",
						"新房,(http://image2.lianjia.com/newhcms)|(http://image2.lianjia.com/x-xf),1");
		otherDomainMap.put("image3.lianjia.com",
				"新房,(http://image3.lianjia.com/xf-resblock),1");
		otherDomainMap.put("qfang.com", "新房,1,6");
		otherDomainMap.put("qfang.com/appapi/v4_2/newHouse",
				"新房,(http://shanghai.qfang.com/appapi/v4_2/newHouse),1");
		otherDomainMap.put("qfang.com/appapi/v4_2/room",
				"二手房,(http://shanghai.qfang.com/appapi/v4_2/room),1");
		otherDomainMap.put("qfang.com/appapi/v4_2/office",
				"商业地产,(http://shanghai.qfang.com/appapi/v4_2/office),1");
		otherDomainMap.put("qfang.com/appapi/v4_2/enums",
				"商业地产,(http://shanghai.qfang.com/appapi/v4_2/enums),1");
		otherDomainMap
				.put("qfang.com/office/rent",
						"商业地产,(http://shanghai\\.qfang\\.com/office/rent/[0-9]+)\\??,1");
		otherDomainMap
				.put("qfang.com/office/sale",
						"商业地产,(http://shanghai\\.qfang\\.com/office/sale/[0-9]+)\\??,1");
	/*	
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
		}*/
	}
	private void setErsHouseMap(Context context){
		ershouseMap
		.put("anjuke.com",
				"anjuke,http://[a-z]+\\.anjuke\\.com/prop/view/([0-9A-Za-z]+),1");
		ershouseMap.put("fang.com",
		"soufang,http://esf\\.[a-z]+\\.fang\\.com/chushou/([0-9_]+)\\.htm,1");
      
		ershouseMap
		.put("lianjia.com",
				"lianjia,http://[a-z]+\\.lianjia\\.com/ershoufang/([0-9A-Za-z]+)\\.html,1");
		ershouseMap
		.put("qfang.com",
				"qfang,http://[a-z]+\\.qfang\\.com/sale/([0-9]+),1");
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
				.put("che168.com",
						"zhijia,http://www\\.che168\\.com/.*/s([0-9]+)$,1");
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
				.put("taoche.com",
						"yiche,http://shenzhen\\.taoche\\.com/.*carid=([0-9]+),1");
		    	
		    	carMap
				.put("sohu.com",
						"sohu,http://db.auto.sohu.com/[a-zA-Z0-9\\-]+/[0-9]+/([0-9]+)|http://open\\.dealer\\.auto\\.sohu\\.com/api/.*seriseId=([0-9]+),1");
		    	
	    }
	
	
	private void setSpecialBv(){
		
		specialbvMap.put("car.autohome.com.cn_对比","http://car.autohome.com.cn/duibi/[a-z]+/carids=[0-9]+,[0-9]+,[0-9]+,[0-9]+,0");
		specialbvMap.put("dealer.autohome.com.cn_试驾","http://dealer.autohome.com.cn/[0-9a-z]+/drive_,0");
		specialbvMap.put("dealer.autohome.com.cn_询价","http://dealer.autohome.com.cn/[0-9a-z]+/order(-|_),1");
		
		specialbvMap.put("price.pcauto.com.cn_对比","http://price.pcauto.com.cn/((comment/)?choose(_photo)?.jsp\\?mid=[0-9]+|pk/sid(-([0-9]+))+.html),0");
		specialbvMap.put("price.pcauto.com.cn_试驾","http://price.pcauto.com.cn/(dealer/commorders/order.jsp\\?(t=2)?|[0-9]+/order2-),0");
		specialbvMap.put("price.pcauto.com.cn_询价","http://price.pcauto.com.cn/(dealer/commorders/order\\.jsp\\?(t=0)?|[0-9]+/order0-),0");
		
		specialbvMap.put("car.bitauto.com_对比","http://car.bitauto.com/[a-z]*duibi/(([0-9]+-[0-9]+/)|(\\?(carIDs|carids)=)),1");
		specialbvMap.put("dealer.bitauto.com_试驾","http://dealer.bitauto.com/([0-9]+/)?shijia/,0");
		specialbvMap.put("dealer.bitauto.com_询价","http://dealer.bitauto.com/([0-9]+/)?zuidijia/,0");
		
		specialbvMap.put("db.auto.sohu.com_对比","http://db.auto.sohu.com/(pk-trim|pk_trimpic).shtml(#((?!(0,0,0,0,0))([0-9]+,[0-9]+,[0-9]+,[0-9]+,[0-9]+))|\\?),0");
		specialbvMap.put("dealer.auto.sohu.com_试驾","http://dealer.auto.sohu.com/[0-9]+/order(/)?\\?.*type=try,0");
		specialbvMap.put("dealer.auto.sohu.com_询价","http://dealer.auto.sohu.com/([0-9]+/order\\?(type=ask)?|xunjia/),0");
	
		specialbvMap.put("newcar.xcar.com.cn_对比","http://newcar.xcar.com.cn/compare(-photo)?/[0-9]+,0");
		specialbvMap.put("newcar.xcar.com.cn_试驾","http://newcar.xcar.com.cn/auto/index.php\\?r=dealerPopw/order.*type=2,0");
		specialbvMap.put("newcar.xcar.com.cn_询价","http://newcar.xcar.com.cn/auto/index.php\\?r=dealerPopw/order.*(type=1|mid=),0");

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
	
	private void setSearchKeyWord(Context context) throws IOException{

		String confuri = context.getConfiguration().get("searchWord");
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
					
					
						try {
							// file.readFields(in);
							in = fs.open(file.getPath());
							BufferedReader bis = new BufferedReader(
									new InputStreamReader(in, "UTF8"));
							String line = "";
							while ((line = bis.readLine()) != null) {
								kwc=line+"#"+kwc;
								//关键字标识串
							}
							  kwc=kwc.substring(0,kwc.length()-1);

						} finally {
							IOUtils.closeStream(in);
						}
					}
				}
			} finally {
				IOUtils.closeStream(in);
			}
		} else {
		
			System.out.println("not exist file !!!!!!");
		}
	
	}
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		setOtherDomainValue(context);
		setDomainValue();
		//setKeyWordRule(context);
		setClassMap(context);
		setCarRule();
		setSpecialBv();
		setCarIndustryRule();
		setErsHouseMap(context);
		setHouseAPPMap();
		setOtherLabel();
		setSearchKeyWord(context);
		setMatchRule();
		setJxqyMap();
		
	}

	private String username = "";
	private String url = "";
	private String domain = "";
	private String sys = "";
	private String accesstime = "";
	private String referDomain = "";
	Map<String,String>appidmap=new HashMap<String, String>();
	private void setHouseAPPMap(){
		appidmap.put("api.anjuke.com", "安居客");
		appidmap.put("soufunappesf.3g.fang.com", "房天下");
		appidmap.put("m.anjuke.com", "安居客");
		appidmap.put("s.anjuke.com", "安居客");
		appidmap.put("m.fang.com", "房天下");
		appidmap.put("api.fang.anjuke.com", "安居客");
		appidmap.put("image2.lianjia.com", "链家");
		appidmap.put("image3.lianjia.com", "链家");
		appidmap.put("shanghai.qfang.com", "q房");
		appidmap.put("m.fangdd.com", "房多多");
		appidmap.put("hd.xf.fangdd.com", "房多多");
		appidmap.put("fs.esf.fangdd.com", "房多多");
		
	}
	
	
	protected void map(LongWritable key,Text value,Context context)
			throws IOException, InterruptedException {
        
        String splitflag= context.getConfiguration().get("splitflag");
		String[] items = value.toString().split(splitflag);
		String choosedValue=context.getConfiguration().get("choosedValue");
		String choosedValues[]=choosedValue.split(",");
		try{
			username = PraseDataUtil.setChooseValue(choosedValues[0], username, items);
		
		
		if("none".equals(username))
			return;
	    url = PraseDataUtil.setChooseValue(choosedValues[1], url, items);//url = items[3];
		if(!url.contains("http"))
			return;
		domain =new URL(url).getHost();
		accesstime = PraseDataUtil.setChooseValue(choosedValues[2], accesstime, items);//accesstime = items[2];
		referDomain = " ";
		sys =" ";
		paraseIndustry(context);
		praseOtherLabel(context);
		praseSearchKeyWork(context);
		praseAppMatch(context);
		}catch(Exception e){
			
		}
		
		}
	private Map<String,String>jxqyInfoMap=new HashMap<String, String>();
	private void setJxqyMap(){
		jxqyInfoMap.put("billing.zlongame.com", "御剑情缘APP");
		jxqyInfoMap.put("static-lo.igg.com", "王国纪元");
		jxqyInfoMap.put("q.qlogo.cn", "火影忍者");
		jxqyInfoMap.put("optsdk.gameyw.netease.com", "阴阳师");
		jxqyInfoMap.put("down.qq.com", "王者荣耀");
		jxqyInfoMap.put("update.dhxy.163.com", "大话西游");
		jxqyInfoMap.put("update.leiting.com", "问道");
		jxqyInfoMap.put("update.qnm.163.com", "倩女幽魂");
		jxqyInfoMap.put("f.aiwaya.cn", "影之刃2");
		jxqyInfoMap.put("member.zlongame.com", "青丘狐传说");
		jxqyInfoMap.put("click.37wan.5ypy.com", "永恒纪元");
		jxqyInfoMap.put("image.qjnn.qq.com", "奇迹暖暖");
		jxqyInfoMap.put("api.wyfx.guangyv.co", "幻剑修仙");
		jxqyInfoMap.put("report.cp01.youyannet.com", "一剑灭仙");
		jxqyInfoMap.put("report.cp01.youyannet.com", "紫青双剑");
		jxqyInfoMap.put("report.lyxz.q-dazzle.com", "云歌情缘");
		jxqyInfoMap.put("report.cp01.youyannet.com", "御剑飞仙");
		jxqyInfoMap.put("register.xiyou.shiyuegame.com", "紫青双剑");
		jxqyInfoMap.put("ios.svr50.zcjoy.com", "少女水浒传");
		jxqyInfoMap.put("cls.mg05.youyannet.com", "梦幻仙侠传奇");
		jxqyInfoMap.put("res1.cdn.youyannet.com", "蜀山御剑情缘");
		jxqyInfoMap.put("report.mg05.youyannet.com", "热血仙侠");
		jxqyInfoMap.put("report.cp01.youyannet.com", "御剑修仙");
	    jxqyInfoMap.put("ioslogin3.xlj.tianyouxi.com", "幻城之剑");
	    jxqyInfoMap.put("qcloud1.huoyugame.com", "御剑江湖");
	}
	private void praseYJQYinfo(String keyword,org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException{
	   SimpleDateFormat dfTime = new SimpleDateFormat("yyyyMMddHHmmss");
		   Date dAccessTime = new Date(Long.parseLong(accesstime.trim()));
		   String strAccessTime = dfTime.format(dAccessTime);
		   String timestamp = strAccessTime.trim();
			long date = 0;
			long hour = 0;
			try {
				date = Long.parseLong(timestamp.substring(0, 8));
				hour = Long.parseLong(timestamp.substring(8, 10));
			} catch (Exception e) {
				;
			}
	
			if (date == 0) {
				return;
			}
			kWs= context.getConfiguration().get("kW");;
		List<String> kWst = Arrays.asList(kWs.split("@@@"));
		
		String k = username + "," + date + "," + hour;
		if(url.equals("http://zqsj.bztj.youkia.net/")){
			context.write(new Text(k + ",御剑情缘官方页" ),
					new Text("1"));
		}
		String mykey=jxqyInfoMap.get(domain);
		if(mykey!=null){
			context.write(new Text(k + "," + mykey),
					new Text("1"));
		}
		for (String temp : kWst) {
			if (keyword.contains(temp)) {
				String outkey=k + "," + keyword;
				context.write(new Text("countDATA_yjqyInfo_" + outkey),
						new Text("1"));
			}
		}
	}
    private void praseAppMatch(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException{
    	String rule=matchMap.get(domain);
    	if(rule!=null){
    		String match=findMathc(url, rule);
    		if(!"".equals(match)){
    		String outkey = username + ","+matchlabelValue.get(match);//result2[5]
			context.write(new Text("countDATA_industry_" + outkey),
					new Text("1"));	
			}
    	}
    }
	private void praseSearchKeyWork(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException{
    	String keyword=CommonFunction.parseKeyWord(url,kwc,domain);
    	if(keyword!=null&&keyword.length()>1){
    		keyword=keyword.replace(",", "").replace("\n", "");
    	
    		String outkey = username + ",搜索关键字," + keyword;//result2[5]
    		praseYJQYinfo(keyword, context);
			context.write(new Text("countDATA_searchkeyword_" + outkey),
					new Text("1"));
    	}
    }
	private void praseOtherLabel(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException{
		String value=otherLabelMap.get(domain);
		if(value!=null){
			String attrName=value.split(",")[0];
			String attrvalue=value.split(",")[1];
			String outkey= username + ","+attrName+"," + attrvalue;//result2[5];
			context.write(new Text("countDATA_industry_" + outkey), new Text(
					"1"));
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
            if(outkey.split(",").length==3)
			context.write(new Text("countDATA_industry_" + outkey), new Text(
					"1"));
			
		}

		
		try {
			
			paraseNewHouse(context);// 解析新房数据
			paraseKeyWord(context);// 解析关键字信息
			paraseOtherHouse(context);// 解析房产行业标签
			paraseErHouse(context);
			paraseNewCar(context);
			paraseSpecialBv(context);// 解析汽车特定行为标签
		    paraseCarIndustry(context);// 解析汽车行业标签
		} catch (Exception e) {
           
		}
	}
	public void paraseErHouse(
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
			String rules = ershouseMap.get(rootDomain);

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
						rules = ershouseMap.get(rootDomain);
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

				String outkey = username + "," + url + "," + match;
				if (!"".equals(match)) {
					context.write(new Text("ershoufang"), new Text(outkey));
					outkey = username + ",房产,二手房";
					context.write(new Text("countDATA_industry_" + outkey),
							new Text("1"));
				}
			}
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
					String outkey = username + "|" + url + "|" + match + "|房产|"
							+ domain + "|" + accesstime + "|" + flag + "|"
							+ referDomain + "|";//
				 
				    	context.write(new Text("otherHouse"), new Text(outkey));
					outkey = username + ",房产," + match;//result2[5]
					context.write(new Text("countDATA_industry_" + outkey),
							new Text("1"));
				
				
					String appid=appidmap.get(domain);
					if(appid!=null){
						outkey = username + ",房产app," + appid;//result2[5]
						context.write(new Text("countDATA_industry_" + outkey),
								new Text("1"));
					}
				
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

				String outkey =username + "," + url + "," + match + ","
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
		
			String[] types={"对比","试驾","询价"};
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
		String[] result = new String[4];
		String source = rules.split(",")[0];
		String regex = rules.split(",")[1];
		String jflag = rules.split(",")[2];
		result[0] = source;
		result[1] = regex;
		result[2] = jflag + "";

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

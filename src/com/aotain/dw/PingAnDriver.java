package com.aotain.dw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PingAnDriver extends Configured implements Tool{
	
	private String urlList = "imanhua.com|kukudm.com|17173.com|pcgames.com.cn|duowan.com|tgbus.com|"
			+ "icbc.com.cn|ccb.com|psbc.com|abchina.com|boc.cn|chinabank.com.cn|95599.cn|cebbank.com|"
			+ "alipay.com|cmbchina.com|cib.com.cn|cgbchina.com.cn|hsbank.com.cn|tenpay.com|cmbc.com.cn|"
			+ "bank.pingan.com|paypassport.suning.com|bank.ecitic.com|bank.pingan.com/geren/touzi|"
			+ "bank.pingan.com/geren/licai|one.pingan.com|icbc.com.cn/icbc/个人金融|icbc.com.cn/icbc/网上理财|"
			+ "buy.ccb.com/financemarket|finance.ccb.com|psbc.com/portal/zh_CN/PersonalFinancing|"
			+ "abchina.com/cn/FinancialService|95599.cn/cn/FinancialService";
	
	public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
		//hbase
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hive-2");  //千万别忘记配置
        conf.set("hbase.zookeeper.property.clientPort","2181");
        
        //设置参数
        //解析的列配置，按列顺序
		conf.set("app.urllist", urlList);  
	    //conf.set("rowkey.index","1,6");
	    
	    @SuppressWarnings("deprecation")
        Job job = Job.getInstance(conf, "PingAn-to-Hbase");
        job.setJarByClass(PingAnDriver.class);
	    //turk.rowkey
	    //conf.set("turk.rowkeys","BSC:ACCESS_CELL");
	    //turk.column
	    //conf.set("turk.column","CALL_DURATION");
	    //turk.outcolumn
	    //conf.set("turk.outcolumn",columnname);
        
        //需要分析的文件目录
        Path in = new Path(arg0[0]);
        
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, in);
        
        job.setMapperClass(PingAnMapper.class);
        job.setReducerClass(PingAnReuceer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        TableMapReduceUtil.initTableReducerJob("pinganurl", PingAnReuceer.class, job);
        
        job.waitForCompletion(true);
        return 0;
    }
	
	
	public static void main(String [] args) throws Exception{
        int mr;
        mr = ToolRunner.run(new Configuration(),new PingAnDriver(),args);
        System.exit(mr);
    }
}

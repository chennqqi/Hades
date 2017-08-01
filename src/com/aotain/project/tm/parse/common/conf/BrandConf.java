package com.aotain.project.tm.parse.common.conf;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.aotain.project.tm.parse.common.pojo.Brand;
import com.aotain.project.tm.parse.common.utils.StringUtil;

/**
 * Ʒ������
 * ����Ʒ�����ݵ�����
 * ����һЩ�ַ�������һЩ�滻Ʒ�ƣ�ȥƷ�ƵȲ���
 * @author Liangsj
 *
 */
public class BrandConf {

	private List<Brand> brands = new ArrayList<Brand>();
	
	{
		// TODO ��������
		/*brands.add(new Brand("","SM","����","SAMSUNG"));
		brands.add(new Brand("������","GALAXY","����","SAMSUNG"));
		brands.add(new Brand("","HUAWEI","��Ϊ","HUAWEI"));
		brands.add(new Brand("","ACER","�곞","ACER"));
		brands.add(new Brand("","","VIVO","VIVO"));*/
	}
	
	/**
	 * �ӱ��ؼ�������
	 * @param confFile �����ļ�·��
	 */
	public void load(String confFile) {
		InputStream in = null;
		try {
			in = new FileInputStream(confFile);
			load(in);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new RuntimeException("���������쳣", e);
		} finally{
			if(null != in) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * ��������
	 * @param in
	 */
	public void load(InputStream in) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(in, "utf-8"));
			String line = null;
			while((line=br.readLine())!=null) {
				line= line.trim();
				String[] arr = line.split(",", 4);
				if(arr.length < 4){
					System.out.println("error:"+line);
					continue;
				}
				brands.add(new Brand(arr[0].trim().toUpperCase(),arr[1].trim().toUpperCase(),arr[2].trim().toUpperCase(),arr[3].trim().toUpperCase()));
			}
			System.out.println("�����ն�Ʒ��������ɣ�size=" + brands.size());
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			try {
				if(br !=null) {
					br.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * ��HDFS��������
	 * @param hdfsFile hdfs�ļ�·��
	 * @param conf
	 */
	public void load(String hdfsFile, Configuration conf) {
		InputStream in = null;
		try {
			System.out.println("����Ʒ������·��:hdfs_path=" + hdfsFile);
			FileSystem fs = FileSystem.get(conf);
			Path  path = new Path(hdfsFile);
			if(fs.exists(path)) {
				for(FileStatus  filestatus : fs.listStatus(path)) {
					System.out.println("����Ʒ������:file=" + filestatus.getPath());
					in = fs.open(filestatus.getPath());
					load(in);
					close(in, null);
				}
			}
			System.out.println("Ʒ�����ݼ������,size:" + brands.size());
			if(brands.size() == 0) {
				throw new RuntimeException("Ʒ������Ϊ�գ�����,hdfs_path="+ hdfsFile);
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("���������쳣", e);
		}finally{
			if(null != in) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public void close(InputStream in, Reader reader) {
		try {
			if(in != null) {
				in.close();
			}
			if(reader != null) {
				reader.close();
			}
			} catch (IOException e) {
		}
	}
	
	/**
	 * ����ַ����а���Ʒ�����������滻ΪӢ��
	 * @return
	 */
	public String replaceCname2Ename(String str, String cname){
		return replace(str, Brand.CNAME, Brand.ENAME, cname);
	}
	
	/**
	 * ����ַ����а�������key���滻ΪӢ��key
	 * @param flag trueʱ�滻ename,falseʱ�滻Ϊkey
	 * @return
	 */
	public String replaceCkey2EKey(String str, String cname){
		if(StringUtil.isNull(cname)){
			return str;
		}
		return replace(str, Brand.CKEY, Brand.EKEY, cname);
	}
	
	public String replaceEkey2EName(String str, String cname){
		if(StringUtil.isNull(cname)){
			return str;
		}
		return replace(str, Brand.EKEY, Brand.ENAME, cname);
	}
	
	/**
	 * ����brands���滻str��AֵΪBֵ
	 * AֵΪbrand�ĵ�brandIdx1������
	 * BֵΪbrand�ĵ�brandIdx2������
	 * @param str
	 * @param brandIdx1 : Brand.CKEY Brand.EKEY Brand.ENAME Brand.CNAME
	 * @param brandIdx2 : Brand.CKEY Brand.EKEY Brand.ENAME Brand.CNAME
	 * @param cname ��cname��Ϊ�գ���ʾ���滻ֻ�滻Ʒ����Ϊcname������
	 * @return
	 */
	private String replace(String str, int brandIdx1, int brandIdx2, String cname){
		if(StringUtil.isNull(str)){
			return str;
		}
		for(Brand b : brands){
			if(cname != null && !b.getCname().equals(cname)){
				continue;
			}
			if(!StringUtil.isNull(b.valueOf(brandIdx2)) && StringUtil.contains(str, b.valueOf(brandIdx1))){
				return str.replaceFirst(b.valueOf(brandIdx1),b.valueOf(brandIdx2));
			}
		}
		return str;
	}
	
	/**
	 * ����brands��ɾ��str��Aֵ
	 * AֵΪbrand�ĵ�brandIdx������
	 * @param str
	 * @param brandIdx : Brand.CKEY Brand.EKEY Brand.ENAME Brand.CNAME
	 * @param cname ��cname��Ϊ�գ���ʾ���滻ֻɾ��Ʒ����Ϊcname������
	 * @return
	 */
	private String remove(String str, int brandIdx, String cname){
		if(StringUtil.isNull(str)){
			return str;
		}
		for(Brand b : brands){
			if(cname != null && !b.getCname().equals(cname)){
				continue;
			}
			if(StringUtil.contains(str, b.valueOf(brandIdx))){
				return str.replace(b.valueOf(brandIdx),"");
			}
		}
		return str;
	}
	
	/**
	 * ȥ���ַ����е�Ʒ�ƣ��������ĺ�Ӣ��
	 * @param str
	 * @return
	 */
	public String rmBrand(String str, String cname){
		return remove(remove(str, Brand.CNAME, null), Brand.ENAME, cname);
	}
	
	/**
	 * ȥ���ַ����е�Ӣ��Ʒ��
	 * @param str
	 * @return
	 */
	public String rmEbrand(String str, String cname){
		return remove(str, Brand.ENAME, cname);
	}
	
	/**
	 * ȥ���ַ����е�����Ʒ��
	 * @param str
	 * @return
	 */
	public String rmCbrand(String str, String cname){
		return remove(str, Brand.CNAME, null);
	}
	
	/**
	 * ȥ��Ʒ�ƹؼ��֣�ǰ����Ҫ����Ʒ��������
	 * @param str
	 * @param brand
	 * @return
	 */
	public String rmKey(String str, String cname){
		if(StringUtil.isNull(cname)){
			return str;
		}
		return remove(remove(str, Brand.CKEY, cname), Brand.EKEY, cname);
	}
	
	/**
	 * ȥ��Ʒ��Ӣ�Ĺؼ��֣�ǰ����Ҫ����Ʒ��������
	 * @param str
	 * @param brand
	 * @return
	 */
	public String rmEKey(String str, String cname){
		if(StringUtil.isNull(cname)){
			return str;
		}
		return remove(str, Brand.EKEY, cname);
	}
	
	/**
	 * �����������е�findIdx���ֶΣ�ֵΪfindValue��Brand
	 * @param findIndx Brand.CKEY Brand.EKEY Brand.CNAME Brand.ENAME
	 * @return
	 */
	public List<Brand> getFromAtts(int findIdx, String findValue){
		if(StringUtil.isNull(findValue)){
			return null;
		}
		List<Brand> result = new ArrayList<Brand>();
		for(Brand b : brands){
			if(b.valueOf(findIdx).equals(findValue)){
				result.add(b);
			}
		}
		return result;
	}
	
	
	/**
	 * �����������е�findIdx���ֶΣ�ֵΪfindValue��Brand
	 * @param findIndx Brand.CKEY Brand.EKEY Brand.CNAME Brand.ENAME
	 * @return
	 */
	public Brand getFromOneAtt(int findIdx, String findValue){
		if(StringUtil.isNull(findValue)){
			return null;
		}
		for(Brand b : brands){
			if(b.valueOf(findIdx).equals(findValue)){
				return b;
			}
		}
		return null;
	}
	
	public void printAll(){
		for(Brand b : brands){
			System.out.println(b);
		}
	}
	
	
	public List<Brand> getBrands() {
		return brands;
	}

	public void setBrands(List<Brand> brands) {
		this.brands = brands;
	}

	public static void main(String[] args) {
		BrandConf conf = new BrandConf();
		conf.load("config/brand_ch.csv");
		String str = "�߲ʺ� ab";
		System.out.println(conf.replaceCkey2EKey(str, "�߲ʺ�"));
		System.out.println(conf.getFromOneAtt(Brand.CNAME, "��֥"));
	}
}
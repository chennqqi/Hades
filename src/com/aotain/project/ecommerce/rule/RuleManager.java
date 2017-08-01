package com.aotain.project.ecommerce.rule;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.aotain.project.ecommerce.pojo.Pair;

/**
 * 存放适配规则
 * Map key为domain，Value为List<POST_FIELD,COMM_FIELD>
 * Map key为域名，Value为List<数据源字段,输出字段>
 * @author Administrator
 *
 */
public class RuleManager {

	private Map<String, Rule> ruleMap = new HashMap<String, Rule>();
	
	/**
	 * 添加规则
	 * @param host 域名
	 * @param dataColumn 数据源字段
	 * @param newDataColumn 输出字段
	 */
	public void addRule(String host, String dataColumn, String newDataColumn){
		Rule rule = ruleMap.get(host);
		if(null == rule) {
			rule = new Rule(host);
			rule.addPair(new Pair(dataColumn, newDataColumn));
			ruleMap.put(host, rule);
		} else {
			rule.addPair(new Pair(dataColumn, newDataColumn));
		}
	}
	
	public boolean isHostExist(String host) {
		return ruleMap.containsKey(host);
	} 
	
	public Rule getRule(String host) {
		return ruleMap.get(host);
	}
	
	public String printRule() {
		StringBuffer sb = new StringBuffer("Rules:");
		Set<String> set = ruleMap.keySet();
		for(String key : set) {
			sb.append("domain:" + key);
			Rule rule = ruleMap.get(key);
			List<Pair> pairs = rule.getColumnPair();
			for(Pair p : pairs) {
				sb.append("\t" + p.getKey() + "->" + p.getValue() + "\n");
			}
		}
		return sb.toString();
	}
	
	public Set<String> getAllHost() {
		return ruleMap.keySet();
	}
	
	/**
	 * 装载配置到内存
	 * @param postRuleIn post_rule.xml输入流，每个domain对应一个urlid
	 * @param postPersonIn post_person.xml输入流， urlid对应POST_FIELD(post表字段名)和COMM_FIELD(输出字段名)
	 * @param domains 指定需要装载的DOMAIN
	 * @param columns 指定需要装载的列
	 * @throws IOException
	 */
	public void loadConfRule(InputStream postRuleIn, InputStream postPersonIn,String[] domains, String[] columns) throws IOException {

		SAXReader saxReader = new SAXReader();
		List<Pair> rules = new ArrayList<Pair>();
		try {
			Document document = saxReader.read(postRuleIn);
			Element rootElement = document.getRootElement();
			List rows = rootElement.elements();
			for (int i = 0, l = rows.size(); i < l; i++) {
				Element row = (Element) rows.get(i);
				String urlId = row.elementText("URL_ID");
				String domain = row.elementText("DOMAIN");
				// 如果限制了domain，只装载domain相关的rule,否则全量装载
				if(null == domains || domains.length == 0) {
					rules.add(new Pair(urlId.trim(), domain.trim()));
				} else {
					for(String domainstr : domains) {
						if(domain.contains(domainstr)) {
							rules.add(new Pair(urlId.trim(), domain.trim()));
							break;
						}
					}
				} 
			}

			document = saxReader.read(postPersonIn);
			rootElement = document.getRootElement();
			rows = rootElement.elements();
			for (int i = 0, l = rows.size(); i < l; i++) {
				Element row = (Element) rows.get(i);
				String urlId = row.elementText("URL_ID");
				String postFileId = row.elementText("POST_FIELD");
				String commFileId = row.elementText("COMM_FIELD");
				
				// 如果限制了列，只装载有对应列的rule,否则全量装载
				boolean add = false;
				if(null == columns || columns.length == 0) {
					add = true;
				}else {
					for(String col : columns) {
						if(commFileId.equalsIgnoreCase(col)) {
							add = true;
						}
					}
				}
				
				if(add) {
					for (Pair p : rules) {
						if (p.getKey().equals(urlId)) {
							addRule(p.getValue(), postFileId, commFileId);
						}
					}
				}
			}
		} catch (DocumentException e) {
			System.err.println("解析文件异常" + e.toString());
			throw new IOException(e);
		}
	}
}

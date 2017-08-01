package com.aotain.common.impl;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.io.IOException;
import java.util.Iterator;

public abstract class OptimizedDataJoinMapperBase extends OptimizedJobBase{
	 protected String inputFile = null;
	 protected JobConf job = null;
	 protected Text inputTag = null;
	 protected Reporter reporter = null;
	 protected CompositeKey outputKey = new CompositeKey();
	 protected BooleanWritable smaller;
	 public void configure(JobConf job) {
	 super.configure(job);
	 this.job = job;
	 this.inputFile = job.get("map.input.file");
	 this.inputTag = generateInputTag(this.inputFile);
	 if(isInputSmaller(this.inputFile)) {
	 smaller = new BooleanWritable(true);
	 outputKey.setOrder(0);
	 } else {
	 smaller = new BooleanWritable(false);
	 outputKey.setOrder(1);
	 }
	 }
	 /**
	 * Determine the source tag based on the input file name.
	 *
	 * @param inputFile
	 * @return the source tag computed from the given file name.
	 */
	 protected abstract Text generateInputTag(String inputFile);
	 /**
	 * Generate an output value. The user code can also perform
	 * projection/filtering. If it decides to discard the input record when
	 * certain conditions are met,it can simply return a null.
	 *
	 * @param o the Map input value
	 * @return an object of OutputValue computed from the given value.
	 */
	 protected abstract OutputValue genMapOutputValue(Object o);
	 /**
	 * Generate a map output key. The user code can compute the key
	 * programmatically, not just selecting the values of some fields. In this
	 * sense, it is more general than the joining capabilities of SQL.
	 *
	 * @param aRecord
	 * @return the group key for the given record
	 */
	 protected abstract String genGroupKey(Object key,
			 OutputValue aRecord);
	 /**
	 *
	 * @param inputFile
	 * @return true if the data from the supplied input file is smaller
	 * than data from the other input file.
	 */
	 protected abstract boolean isInputSmaller(String inputFile);
	 
	 public void map(Object key, Object value,OutputCollector output, Reporter reporter) throws IOException 
	 {
		 
		 if (this.reporter == null) {
			 this.reporter = reporter;
		 }
		 addLongValue("totalCount", 1);
		 OutputValue aRecord = genMapOutputValue(value);
		 if (aRecord == null) {
			 addLongValue("discardedCount", 1);
			 return;
		 }
		 aRecord.setSmaller(smaller);
		 String groupKey = genGroupKey(key, aRecord);
		 if (groupKey == null) {
			 addLongValue("nullGroupKeyCount", 1);
			 return;
		 }
		 outputKey.setKey(groupKey);
		 output.collect(outputKey, aRecord);
		 addLongValue("collectedCount", 1);
	 }
	 	
	 
	 public void close() throws IOException {
	 		if (this.reporter != null) {
	 			this.reporter.setStatus(super.getReport());
	 		}
	 }
	 	
	 public void reduce(Object arg0, Iterator arg1,
			 OutputCollector arg2, Reporter arg3) throws IOException {
		 // TODO Auto-generated method stub
	}
}

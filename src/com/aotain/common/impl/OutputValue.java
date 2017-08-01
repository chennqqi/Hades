package com.aotain.common.impl;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;

public abstract class OutputValue implements Writable{
	 protected BooleanWritable smaller;
	 public OutputValue() {
	 this.smaller = new BooleanWritable(false);
	 }
	 public BooleanWritable isSmaller() {
	 return smaller;
	 }
	 public void setSmaller(BooleanWritable smaller) {
	 this.smaller = smaller;
	 }
	 public abstract Writable getData();
	 public OutputValue clone(JobConf job) {
	 return WritableUtils.clone(this, job);
	 }
	 @Override
	 public String toString() {
	 return ToStringBuilder.reflectionToString(this);
	 }
}

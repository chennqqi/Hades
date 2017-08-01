package com.aotain.common.impl;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import java.io.IOException;
import java.util.*;

public abstract class OptimizedDataJoinReducerBase extends OptimizedJobBase{
	 protected Reporter reporter = null;
	 private long maxNumOfValuesPerGroup = 100;
	 protected long largestNumOfValues = 0;
	 protected long numOfValues = 0;
	 protected long collected = 0;
	 protected JobConf job;
	 public void close() throws IOException {
	 if (this.reporter != null) {
	 this.reporter.setStatus(super.getReport());
	 }
	 }
	 public void configure(JobConf job) {
	 super.configure(job);
	 this.job = job;
	 this.maxNumOfValuesPerGroup =
	 job.getLong("datajoin.maxNumOfValuesPerGroup", 100);
	 }
	 public void reduce(Object key, Iterator values,
	 OutputCollector output, Reporter reporter)
	 throws IOException {
	 if (this.reporter == null) {
	 this.reporter = reporter;
	 }
	 CompositeKey k = (CompositeKey) key;
	 System.out.println("K[" + k + "]");
	 List<OutputValue> smaller =
	 new ArrayList<OutputValue>();
	 this.numOfValues = 0;
	 while (values.hasNext()) {
	 numOfValues++;
	 Object value = values.next();
	 System.out.println(" V[" + value + "]");
	 if (this.numOfValues % 100 == 0) {
	 reporter.setStatus("key: " + key.toString() + " numOfValues: "
	 + this.numOfValues);
	 }
	 if (this.numOfValues > this.maxNumOfValuesPerGroup) {
	 continue;
	 }
	 OutputValue cloned =
	 ((OutputValue) value).clone(job);
	 if (cloned.isSmaller().get()) {
	 System.out.println("Adding to smaller coll");
	 smaller.add(cloned);
	 } else {
	 System.out.println("Join/collect");
	 joinAndCollect(k, smaller, cloned, output, reporter);
	 }
	 }
	 if (this.numOfValues > this.largestNumOfValues) {
	 this.largestNumOfValues = numOfValues;
	 LOG.info("key: " + key.toString() + " this.largestNumOfValues: "
	 + this.largestNumOfValues);
	 }
	 addLongValue("groupCount", 1);
	 }
	 /**
	 * Join the list of the value lists, and collect the results.
	 *
	 * @param key
	 * @param smaller
	 * @param value
	 * @param output
	 * @param reporter
	 * @throws IOException
	 */
	 private void joinAndCollect(CompositeKey key,
	 List<OutputValue> smaller,
	 OutputValue value,
	 OutputCollector output,
	 Reporter reporter)
	 throws IOException {
	 if (smaller.size() < 1) {
	 OutputValue combined =
	 combine(key.getKey(), null, value);
	 collect(key, combined, output, reporter);
	 } else {
	 for (OutputValue small : smaller) {
	 OutputValue combined =
	 combine(key.getKey(), small, value);
	 collect(key, combined, output, reporter);
	 }
	 }
	 }
	 private static Text outputKey = new Text();
	 private void collect(CompositeKey key,
	 OutputValue combined,
	 OutputCollector output,
	 Reporter reporter) throws IOException {
	 this.collected += 1;
	 addLongValue("collectedCount", 1);
	 if (combined != null) {
	 outputKey.set(key.getKey());
	 output.collect(outputKey, combined.getData());
	 reporter.setStatus(
	 "key: " + key.toString() + " collected: " + collected);
	 addLongValue("actuallyCollectedCount", 1);
	 }
	 }
	 /**
	 * @param key
	 * @param smallValue
	 * @param largeValue
	 * @return combined value derived from values of the sources
	 */
	 protected abstract OutputValue combine(String key,
	 OutputValue smallValue,
	 OutputValue largeValue);
	 public void map(Object arg0, Object arg1, OutputCollector arg2,
	 Reporter arg3) throws IOException {
	 throw new IOException("Unsupported operation");
	 }
}

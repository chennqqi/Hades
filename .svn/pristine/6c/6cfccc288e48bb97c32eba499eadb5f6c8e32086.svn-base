package com.aotain.common.impl;

import org.apache.commons.logging.*;
import org.apache.hadoop.mapred.*;
import java.util.*;
import java.util.Map.Entry;

public abstract class OptimizedJobBase  implements Mapper, Reducer {

	public static final Log LOG = LogFactory.getLog("optimizeddatajoin.job");
	private SortedMap<Object, Long> longCounters = null;
	/**
	* Increment the given counter by the given incremental value If the counter
	* does not exist, one is created with value 0.
	*
	* @param name
	* the counter name
	* @param inc
	* the incremental value
	* @return the updated value.
	*/
	protected Long addLongValue(Object name, long inc) {
	Long val = this.longCounters.get(name);
	Long retv;
	if (val == null) {
	retv = inc;
	} else {
	retv = val + inc;
	}
	this.longCounters.put(name, retv);
	return retv;
	}
	/**
	* log the counters
	*
	*/
	protected String getReport() {
	StringBuilder sb = new StringBuilder();
	for (Entry<Object, Long> e: this.longCounters.entrySet()) {
	sb.append(e.getKey().toString())
	.append("\t")
	.append(e.getValue())
	.append("\n");
	}
	return sb.toString();
	}
	/**
	* Initializes a new instance from a {@link org.apache.hadoop.mapred.JobConf}.
	*
	* @param job
	* the configuration
	*/
	public void configure(JobConf job) {
	this.longCounters = new TreeMap<Object, Long>();
	}

}

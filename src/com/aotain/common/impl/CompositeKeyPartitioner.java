package com.aotain.common.impl;

import org.apache.hadoop.mapred.*;

public class CompositeKeyPartitioner implements
		Partitioner<CompositeKey, OutputValue>{

		public int getPartition(CompositeKey key, OutputValue value,
		int numPartitions) {
		return Math.abs(key.getKey().hashCode() * 127) % numPartitions;
		}
		
		public void configure(JobConf job) {
		}
}

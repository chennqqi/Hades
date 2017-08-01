package com.aotain.common;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TaggedWritable extends TaggedMapOutput {
	
	private Text data;
	
	public TaggedWritable(){  
		// 必须有,否则反序列化时出错
		this.data = new Text("");
	}
	
	public TaggedWritable(Text data){  
		this.data = data;
	}
	
	@Override
	public Writable getData() {
	return data;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.tag.readFields(in);
        this.data.readFields(in);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		this.tag.write(out);
        this.data.write(out);
	}

}

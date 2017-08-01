package com.aotain.project.gdtelecom;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserAttReducer extends Reducer<Text, Text, Text, Text>
{
  public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
    throws IOException, InterruptedException
  {
    Text v = new Text();

    long cnt = 0L;
    long maxdays = 0L;
    String maxdate = "0";
    boolean flag = false;
    for (Text value : values)
      try
      {
        String[] items = value.toString().trim().split(",");
        cnt += Long.parseLong(items[0]);
        if (Long.parseLong(maxdate) < Long.parseLong(items[2]))
          maxdate = items[2];
        if (maxdays < Long.parseLong(items[1]))
          maxdays = Long.parseLong(items[1]);
        if ("0".equals(items[1]))
          flag = true;
      }
      catch (Exception Exception)
      {
      }
    if (flag) maxdays += 1L;

    v.set(String.format("%s,%s,%s,%s,", new Object[] { key.toString().trim(), Long.valueOf(cnt), Long.valueOf(maxdays), maxdate }));
    context.write(v, new Text(""));
  }
}
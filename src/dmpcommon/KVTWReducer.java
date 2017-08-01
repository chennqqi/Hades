package dmpcommon;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KVTWReducer extends Reducer<Text, Text, Text, Text>
{
  private Text outkey = new Text();
  private Text outvalue = new Text("");
  
  public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
    throws IOException, InterruptedException
  {
    long cnt = 0L;
    boolean flag = false;
    int days = 0;
    String maxdate = "0";

    for (Text value : values)
      try
      {
        String[] items = value.toString().trim().split(",");
        cnt += Long.parseLong(items[0]);
        days += Integer.parseInt((items[1]));
        if(items[1].equals("0"))
        	flag = true;
        if (Long.parseLong(maxdate) < Long.parseLong(items[2]))
          maxdate = items[2];
      }
      catch (Exception Exception)
      {
      }
    days = flag ? days + 1 : days;
    outkey.set(key.toString()+","+cnt+","+days+","+maxdate+",");
//    outvalue.set(cnt+","+days+","+maxdate);
    context.write(outkey, outvalue);
  }
}
package dmpcommon;

import com.aotain.common.ObjectSerializer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KVTWMapper extends Mapper<LongWritable, Text, Text, Text>
{
    private String date;
    private Map<String,Integer> mapPeriod = new HashMap<String,Integer>();
    
    private Text outkey = new Text();
    private Text outvalue = new Text();
    
    private SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		date = context.getConfiguration().get("date");
		mapPeriod = (Map<String, Integer>) ObjectSerializer.deserialize(context.getConfiguration().get("KvPeriod"));
	}

	
  public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
    throws IOException, InterruptedException
  {
    String vret = value.toString();
    String userid,attcode,attvalue,fqvect,lastdate;
    int period = 0, days = 0;
    long day1,day2,daycut,freq = 0;
    try
    {
      String[] items = vret.split(",", -1);
      String[] cells, units;

      if (items.length == 6)//新数据
      {
    	userid = items[0];
    	attcode = items[1];
    	attvalue = items[2];
    	fqvect = items[3];
    	lastdate = items[4];
    	
    	if(mapPeriod.get(attcode) != null)
    	{
    		period = mapPeriod.get(attcode);
    		if(period == 0)
    		{
    			cells = fqvect.split("_",-1);
    			for(int i = 0; i < cells.length; i++)
    			{
        			units = cells[i].split(":",-1);
    				freq += Integer.parseInt(units[1]);
    			}
    			outkey.set(userid+","+attcode+","+attvalue);
    			outvalue.set(freq+",0,"+lastdate);
    			context.write(outkey,outvalue);
    		}
    		else
    		{

    	        Date date1 = format.parse(lastdate.substring(0, 8));
    	        Date date2 = format.parse(date);
    	        day1 = date1.getTime();
    	        day2 = date2.getTime();
    	        daycut = (day2 - day1) / 1000L / 3600L / 24L;
    	        if(daycut < period)
    	        {
        			cells = fqvect.split("_",-1);
        			for(int i = 0; i < cells.length; i++)
        			{
            			units = cells[i].split(":",-1);
        				freq += Integer.parseInt(units[1]);
        			}
	    			outkey.set(userid+","+attcode+","+attvalue);
	    			outvalue.set(freq+",0,"+lastdate);
	    			context.write(outkey,outvalue);
    	        }
    		}
    	}
      }
      else if (items.length == 7)//历史数据
      {
    	  userid = items[0];
    	  attcode = items[1];
    	  attvalue = items[2];
          freq = Long.parseLong(items[3]);
          days = Integer.parseInt(items[4]);
          lastdate = items[5];
        	
        	if(mapPeriod.get(attcode) != null)
        	{
        		period = mapPeriod.get(attcode);
        		if(period == 0)
        		{
        			outkey.set(userid+","+attcode+","+attvalue);
        			outvalue.set(freq+","+days+","+lastdate);
        			context.write(outkey,outvalue);
        		}
        		else
        		{
        	        Date date1 = format.parse(lastdate.substring(0, 8));
        	        Date date2 = format.parse(date);
        	        day1 = date1.getTime();
        	        day2 = date2.getTime();
        	        daycut = (day2 - day1) / 1000L / 3600L / 24L;
        	        if(daycut < period)
        	        {
            			outkey.set(userid+","+attcode+","+attvalue);
            			outvalue.set(freq+","+days+","+lastdate);
            			context.write(outkey,outvalue);
        	        }
        		}
        	}
      }
    }
    catch (Exception Exception)
    {
    }
  }
}
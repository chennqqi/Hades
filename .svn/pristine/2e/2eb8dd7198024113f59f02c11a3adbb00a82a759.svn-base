package com.aotain.common;


public class HbaseExportToFile {

	public void ExportFile(String tablename,String filepath) throws Exception
	{
		
	}
	
	/**
	 * 0:tablename
	 * 1:columns ,·Ö¸î  aa,bb,cc 
	 * 2:output file
	 * 3:startKey
	 * 4:endKey
	 * @param args
	 */
	public static void main(String[] args){
		try{
			//HbaseExportToFile export = new HbaseExportToFile();
			if(args.length != 5)
			{
				System.err.printf("Usage:<tablename><columns><output file><startKey><endKey>");
                return;  
			}
			
			String[] columns = null;
			String tablename = args[0];
			if(!args[1].equals("*"))
			{
				columns = args[1].split(",",-1);
			}
			String filepath = args[2];
			String startKey = args[3];
			String endKey = args[4];
			HbaseCommon.GetDataToFile(tablename, startKey, endKey, columns, filepath);
			System.out.println("OK!!!!!!");
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}

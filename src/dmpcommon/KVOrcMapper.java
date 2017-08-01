package dmpcommon;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;

public class KVOrcMapper extends KVMapper<NullWritable,OrcStruct>{
	  
	private  String inputSchema = "";
	static StructObjectInspector inputOI;

		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			inputSchema = context.getConfiguration().get("inputSchema");
			TypeInfo tfin = TypeInfoUtils
					.getTypeInfoFromTypeString(inputSchema);
			inputOI = (StructObjectInspector) OrcStruct
					.createObjectInspector(tfin);
			super.setup(context);
		}
		
	@Override
      public void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException{                         
			List<Object> ilst = inputOI.getStructFieldsDataAsList(value);
			for(int i = 0; i < ilst.size(); i++)
			{
				vret += ilst.get(i) + fieldsplit;
			}
			super.handle(context);
	  }
}

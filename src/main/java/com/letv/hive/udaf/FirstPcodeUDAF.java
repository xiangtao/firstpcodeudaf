package com.letv.hive.udaf;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

/** 
 * 取最小pcode
 */
public class FirstPcodeUDAF extends AbstractGenericUDAFResolver {

	static final Log LOG = LogFactory.getLog(FirstPcodeUDAF.class.getName());

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
			throws SemanticException {
		if (parameters.length != 2) {
			throw new UDFArgumentTypeException(parameters.length - 2,
					"Exactly three argument is expected.");
		}
		return new GenericUDAFFirstpcodeEvaluator();
	}

	public static class GenericUDAFFirstpcodeEvaluator extends
			GenericUDAFEvaluator {
		private PrimitiveObjectInspector inputOI;
		private PrimitiveObjectInspector inputOI1;
		
		private StandardMapObjectInspector mapOI ;

		public GenericUDAFFirstpcodeEvaluator() {
		}

		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters)
				throws HiveException {
			super.init(m, parameters);
			
			if (m == Mode.PARTIAL1 ) {
				inputOI = (PrimitiveObjectInspector) parameters[0];
				inputOI1 = (PrimitiveObjectInspector) parameters[1];
				return ObjectInspectorFactory
						.getStandardMapObjectInspector((PrimitiveObjectInspector) ObjectInspectorUtils
								.getStandardObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
								,ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector));
			}else if (m == Mode.PARTIAL2) {
				mapOI = (StandardMapObjectInspector) parameters[0];
				return ObjectInspectorFactory
						.getStandardMapObjectInspector((PrimitiveObjectInspector) ObjectInspectorUtils
								.getStandardObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
								,ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector));
			}else if(m == Mode.FINAL){
				mapOI = (StandardMapObjectInspector) parameters[0];
				return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			}else if(m == Mode.COMPLETE){
				inputOI = (PrimitiveObjectInspector) parameters[0];
				inputOI1 = (PrimitiveObjectInspector) parameters[1];
				return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			}else {
				throw new RuntimeException("Mode Exception");
			}
		}

		public static class MapAggregationBuffer  implements AggregationBuffer {
			Map<String,Long[]> buffer;
		}

		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			MapAggregationBuffer result = new MapAggregationBuffer();
			reset(result);
			return result;
		}

		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			MapAggregationBuffer myagg = (MapAggregationBuffer) agg;
			myagg.buffer = new HashMap<String,Long[]>();
		}


		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters)
				throws HiveException {
			    assert (parameters.length == 2);
			    
				Object parmpcode = parameters[0];
				Object parmMinT = parameters[1];
				
				String inptpcodeStr = parmpcode==null?"":PrimitiveObjectInspectorUtils.getString(parmpcode, inputOI);
				inptpcodeStr = StringUtils.trimToEmpty(inptpcodeStr);
				String inptmintStr = parmMinT==null?"":PrimitiveObjectInspectorUtils.getString(parmMinT, inputOI1);
				inptmintStr = StringUtils.trimToEmpty(inptmintStr);
				long pt = -1;
				if(inptmintStr != null && !"".equals(inptmintStr)){
					if(NumberUtils.isNumber(inptmintStr)){
						pt = NumberUtils.toLong(inptmintStr, -1L);
					}
				}
				//过滤 "" 和 "-"
				if(StringUtils.equals(inptpcodeStr, "") || StringUtils.equals(inptpcodeStr, "-")){
					return;
				}
				
				addInMap(agg, inptpcodeStr, 1,pt);
				
		}

		private void addInMap(AggregationBuffer agg, String inptpcodeStr,
				long num,long pt) {
			MapAggregationBuffer myagg = (MapAggregationBuffer) agg;
			Long[] countAndMinTime = myagg.buffer.get(inptpcodeStr);
			if(countAndMinTime == null){
				countAndMinTime = new Long[2];
				countAndMinTime[0]=num;
				countAndMinTime[1]=pt;
				myagg.buffer.put(inptpcodeStr, countAndMinTime);
			}else{
				countAndMinTime[0]=countAndMinTime[0]+num;
				countAndMinTime[1]=countAndMinTime[1]==-1?pt:
					pt==-1?countAndMinTime[1]:NumberUtils.min(pt,countAndMinTime[1],Long.MAX_VALUE);
			}
		}

		@Override
		public Object terminatePartial(AggregationBuffer agg)
				throws HiveException {
			MapAggregationBuffer myagg = (MapAggregationBuffer) agg;
			return myagg.buffer;
		}

		@Override
		public void merge(AggregationBuffer agg, Object partial)
				throws HiveException {
			if (partial != null) {
				MapAggregationBuffer myagg = (MapAggregationBuffer) agg;
				Map partialResult = mapOI.getMap(partial) ;
				
				for(Object key : partialResult.keySet()){
					Object val = partialResult.get(key);
					//org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray cannot be cast to [Ljava.lang.Object;
					LazyBinaryListObjectInspector lazyBinListOI = (LazyBinaryListObjectInspector)mapOI.getMapValueObjectInspector();
					//mapOI.getMapValueElement(data, key);
					LongWritable   o1 = (LongWritable)lazyBinListOI.getListElement(val, 0);
					LongWritable  o2 =  (LongWritable)lazyBinListOI.getListElement(val, 0);
					addInMap(myagg,key.toString(),
							o1.get()
							,o2.get());
				}
			}
		}

		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			MapAggregationBuffer myagg = (MapAggregationBuffer) agg;
			String maxcount_pcode = null;
			long maxCount = 0;
			
			String mintime_pcode = null;
			long minTime = -1;
			for(Map.Entry<String,Long[]> entry :myagg.buffer.entrySet()){
				Long[] countAndMinTime = entry.getValue();
				if(countAndMinTime[1]!=-1 && minTime!=-1 && countAndMinTime[1]<minTime){
					minTime = countAndMinTime[1];
					mintime_pcode = entry.getKey();
				}
				if(countAndMinTime[0]>maxCount){
					maxCount = countAndMinTime[0];
					maxcount_pcode = entry.getKey();
				}
				
			}
			//找到时间最小的pcode
			if(minTime > -1){
				return mintime_pcode;
			}else{
				//否则取countAndMinTime[0] 最多的
				return maxcount_pcode;
			}
		}
	}

}

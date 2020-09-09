package function;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author zhangjun 欢迎关注我的公众号[大数据技术与应用实战],获取更多精彩实战内容
 * <p>
 * 使用自定义聚合函数计算网站TP
 */
public class UdafTP{
	public static void main(String[] args) throws Exception{
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
		tenv.registerFunction("mytp", new TP());
		String sql = "CREATE TABLE source (\n" +
		             " response_time INT,\n" +
		             " ts AS localtimestamp,\n" +
		             " WATERMARK FOR ts AS ts," +
		             "proctime as proctime()\n" +
		             ") WITH (\n" +
		             " 'connector' = 'datagen',\n" +
		             " 'rows-per-second'='1000',\n" +
		             " 'fields.response_time.min'='1',\n" +
		             " 'fields.response_time.max'='1000'" +
		             ")";

		tenv.executeSql(sql);

		String sqlSelect =
				"select TUMBLE_START(proctime,INTERVAL '1' SECOND)  as starttime,mytp(response_time,50) from source" +
				" group by TUMBLE(proctime,INTERVAL '1' SECOND)";

		Table table = tenv.sqlQuery(sqlSelect);
		tenv.toAppendStream(table, Row.class).print();
		env.execute();
	}

	public static class TP extends AggregateFunction<Integer,TPAccum>{

		@Override
		public TPAccum createAccumulator(){
			return new TPAccum();
		}

		@Override
		public Integer getValue(TPAccum acc){
			if (acc.map.size() == 0){
				return null;
			} else {
				Map<Integer,Integer> map = new TreeMap<>(acc.map);
				int sum = map.values().stream().reduce(0, Integer::sum);

				int tp = acc.tp;
				int responseTime = 0;
				int p = 0;
				Double d = sum * (tp / 100D);
				for (Map.Entry<Integer,Integer> entry: map.entrySet()){
					p += entry.getValue();
					int position = d.intValue() - 1;
					if (p >= position){
						responseTime = entry.getKey();
						break;
					}

				}
				return responseTime;
			}
		}

		public void accumulate(TPAccum acc, Integer iValue, Integer tp){
			acc.tp = tp;
			if (acc.map.containsKey(iValue)){
				acc.map.put(iValue, acc.map.get(iValue) + 1);
			} else {
				acc.map.put(iValue, 1);
			}
		}

	}

	public static class TPAccum{
		public Integer tp;
		public Map<Integer,Integer> map = new HashMap<>();
	}

}



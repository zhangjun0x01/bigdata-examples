package sql.function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhangjun 欢迎关注我的公众号[大数据技术与应用实战],获取更多精彩实战内容
 * <p>
 * 自定义TableFunction
 */
public class CustomTableFunction{
	public static void main(String[] args) throws Exception{
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
		                                                    .useBlinkPlanner()
		                                                    .inStreamingMode()
		                                                    .build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);

		tEnv.registerFunction("split", new Split(" "));
		tEnv.registerFunction("duplicator", new DuplicatorFunction());
		tEnv.registerFunction("flatten", new FlattenFunction());

		List<Tuple2<Long,String>> ordersData = new ArrayList<>();
		ordersData.add(Tuple2.of(2L, "Euro"));
		ordersData.add(Tuple2.of(1L, "US Dollar"));
		ordersData.add(Tuple2.of(50L, "Yen"));
		ordersData.add(Tuple2.of(3L, "Euro"));

		DataStream<Tuple2<Long,String>> ordersDataStream = env.fromCollection(ordersData);
		Table orders = tEnv.fromDataStream(ordersDataStream, "amount, currency, proctime.proctime");
		tEnv.registerTable("Orders", orders);

		//使用left join
		Table result = tEnv.sqlQuery(
				"SELECT o.currency, T.word, T.length FROM Orders as o LEFT JOIN " +
				"LATERAL TABLE(split(currency)) as T(word, length) ON TRUE");
		tEnv.toAppendStream(result, Row.class).print();

		//----------------------------

		String sql = "SELECT o.currency, T.word, T.length FROM Orders as o ," +
		             " LATERAL TABLE(split(currency)) as T(word, length)";
		Table result1 = tEnv.sqlQuery(sql);
		tEnv.toAppendStream(result1, Row.class).print();

		//---------------------------
		//多种类型参数

		String sql2 = "SELECT * FROM Orders as o , " +
		              "LATERAL TABLE(duplicator(amount))," +
		              "LATERAL TABLE(duplicator(currency))";
		Table result2 = tEnv.sqlQuery(sql2);
		tEnv.toAppendStream(result2, Row.class).print();

		//----------------------------
		//不固定参数查询

		String sql3 = "SELECT * FROM Orders as o , " +
		              "LATERAL TABLE(flatten(100,200,300))";
		Table result3 = tEnv.sqlQuery(sql3);
		tEnv.toAppendStream(result3, Row.class).print();

		env.execute();
	}

	public static class Split extends TableFunction<Tuple2<String,Integer>>{
		private String separator = ",";

		public Split(String separator){
			this.separator = separator;
		}

		public void eval(String str){
			for (String s: str.split(separator)){
				collect(new Tuple2<String,Integer>(s, s.length()));
			}
		}
	}

	/**
	 * 注册多个eval方法，接收long或者字符串类型的参数，然后将他们转成string类型
	 */
	public static class DuplicatorFunction extends TableFunction<String>{
		public void eval(Long i){
			eval(String.valueOf(i));
		}

		public void eval(String s){
			collect(s);
		}
	}

	/**
	 * 接收不固定个数的int型参数,然后将所有数据依次返回
	 */
	public static class FlattenFunction extends TableFunction<Integer>{
		public void eval(Integer... args){
			for (Integer i: args){
				collect(i);
			}
		}
	}

	/**
	 * 通过注册指定返回值类型，flink 1.11 版本开始支持
	 */
//	@FunctionHint(output = @DataTypeHint("ROW< i INT, s STRING >"))
//	public static class DuplicatorFunction1 extends TableFunction<Row>{
//		public void eval(Integer i, String s){
//			collect(Row.of(i, s));
//			collect(Row.of(i, s));
//		}
//	}

}

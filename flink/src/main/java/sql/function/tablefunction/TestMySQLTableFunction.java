package sql.function.tablefunction;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @author zhangjun 欢迎关注我的公众号 [大数据技术与应用实战],获取更多精彩实战内容
 * <p>
 * 测试mysql维表功能，我们主要是讲代码是怎么实现的，DDL
 */
public class TestMySQLTableFunction{
	public static void main(String[] args) throws Exception{

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		//维表的字段，字段名称和类型要和mysql的一样，否则会查不出来
		String[] fieldNames = new String[]{"cuccency", "rate", "id", "province"};
		TypeInformation[] fieldTypes = new TypeInformation[]{
				Types.STRING(), Types.INT(), Types.INT(), Types.STRING()};
		RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);
		String[] primaryKeys = new String[]{"id"};
		MySQLTableFunction mysql = new MySQLTableFunction(
				rowTypeInfo,
				"jdbc:mysql://localhost/test",
				"root",
				"root",
				"product1",
				primaryKeys);
		tEnv.registerFunction("mysql", mysql);
		//        省市、id、datestamp、date、计数,
		DataStream<Tuple5<String,Integer,Long,String,Long>> data = env.addSource(new GeneUISource())
		                                                              .map(new MapFunction<List,Tuple5<String,Integer,Long,String,Long>>(){
			                                                              @Override
			                                                              public Tuple5<String,Integer,Long,String,Long> map(
					                                                              List value) throws Exception{
				                                                              return new Tuple5<>(
						                                                              value.get(0)
						                                                                   .toString(),
						                                                              Integer.parseInt(
								                                                              value.get(
										                                                              1)
								                                                                   .toString()),
						                                                              Long.parseLong(
								                                                              value.get(
										                                                              2)
								                                                                   .toString()),
						                                                              value.get(3)
						                                                                   .toString(),
						                                                              Long.parseLong(
								                                                              value.get(
										                                                              4)
								                                                                   .toString()));
			                                                              }
		                                                              });

		tEnv.registerDataStream("userinfo", data, "province,id,datastamp,date,num");

		String sql = "SELECT u.* , r.* , u.num * r.rate FROM userinfo  as u " +
		             " left JOIN LATERAL TABLE(mysql(u.id)) as r ON true";
		Table result = tEnv.sqlQuery(sql);
		tEnv.toRetractStream(result, Row.class).print();
		env.execute();
	}
}

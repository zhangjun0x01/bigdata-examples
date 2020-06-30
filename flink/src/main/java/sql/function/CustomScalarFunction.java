package sql.function;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import java.util.stream.Stream;

/**
 * @author zhangjun 欢迎关注我的公众号[大数据技术与应用实战],获取更多精彩实战内容
 * <p>
 * 自定义ScalarFunction
 */
public class CustomScalarFunction{
	public static void main(String[] args) throws Exception{
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		//通过程序的方式来注册函数
		SumFunction sumFunction = new SumFunction();
		tableEnv.registerFunction("mysum", sumFunction);
		Table table1 = tableEnv.sqlQuery("select mysum(1,2)");
		tableEnv.toAppendStream(table1, Row.class).print();

		//------------------------------------------------------

		//通过sql的方式来注册函数
		String className = SumFunction.class.getName();
		String sql = "create temporary function default_catalog.default_database.mysum1" +
		             " as '" + className + "'";
		tableEnv.sqlUpdate(sql);
		Table table2 = tableEnv.sqlQuery("select mysum1(3,4)");
		tableEnv.toAppendStream(table2, Row.class).print();

		//------------------------------------------------------

		//列出来所有的函数，看是否包含我们定义的函数
		String[] functions = tableEnv.listFunctions();
		Stream.of(functions).filter(f->f.startsWith("mysum")).forEach(System.out::println);

		//---------------------------------------------------

		//接收非空的int或者boolean类型
		StringifyFunction stringifyFunction = new StringifyFunction();
		tableEnv.registerFunction("myToString", stringifyFunction);
		Table table3 = tableEnv.sqlQuery("select myToString(1) ,myToString(false)  ");
		tableEnv.toAppendStream(table3, Row.class).print();


		//接收任何类型的值，然后把它们转成string
		StringifyFunction1 stringifyFunction1 = new StringifyFunction1();
		tableEnv.registerFunction("myToStringAny", stringifyFunction1);
		Table table4 = tableEnv.sqlQuery("select myToStringAny(1) ,myToStringAny(false),myToStringAny('aaa')  ");
		tableEnv.toAppendStream(table4, Row.class).print();

		env.execute("CustomScalarFunction");
	}

	/**
	 * 接受两个int类型的参数，然后返回计算的sum值
	 */
	public static class SumFunction extends ScalarFunction{
		public Integer eval(Integer a, Integer b){
			return a + b;
		}
	}

	/**
	 * 接收非空的int或者boolean类型
	 */
	public static class StringifyFunction extends ScalarFunction{
		public String eval(int i){
			return String.valueOf(i);
		}

		public String eval(boolean b){
			return String.valueOf(b);
		}
	}

	/**
	 * 接收任何类型的值，然后把它们转成string
	 */
	public static class StringifyFunction1 extends ScalarFunction{
		public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o){
			return o.toString();
		}
	}

}

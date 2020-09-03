package dimension;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author zhangjun 欢迎关注我的公众号[大数据技术与应用实战],获取更多精彩实战内容
 * <p>
 * jdbc lookup 维表使用
 */
public class JdbcDim{
	public static void main(String[] args) throws Exception{
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		String sourceSql = "CREATE TABLE datagen (\n" +
		                   " userid int,\n" +
		                   " proctime as PROCTIME()\n" +
		                   ") WITH (\n" +
		                   " 'connector' = 'datagen',\n" +
		                   " 'rows-per-second'='100',\n" +
		                   " 'fields.userid.kind'='random',\n" +
		                   " 'fields.userid.min'='1',\n" +
		                   " 'fields.userid.max'='100'\n" +
		                   ")";

		tEnv.executeSql(sourceSql);


		String mysqlDDL = "CREATE TABLE dim_mysql (\n" +
		                  "  id int,\n" +
		                  "  name STRING,\n" +
		                  "  PRIMARY KEY (id) NOT ENFORCED\n" +
		                  ") WITH (\n" +
		                  "   'connector' = 'jdbc',\n" +
		                  "   'url' = 'jdbc:mysql://localhost:3306/test',\n" +
		                  "   'table-name' = 'userinfo',\n" +
		                  "   'username' = 'root',\n" +
		                  "   'password' = 'root'\n" +
		                  ")";

		tEnv.executeSql(mysqlDDL);

		String sql = "SELECT * FROM datagen\n" +
		             "LEFT JOIN dim_mysql FOR SYSTEM_TIME AS OF datagen.proctime \n" +
		             "ON datagen.userid = dim_mysql.id";

		Table table = tEnv.sqlQuery(sql);
		tEnv.toAppendStream(table,Row.class).print();

		env.execute();
	}
}

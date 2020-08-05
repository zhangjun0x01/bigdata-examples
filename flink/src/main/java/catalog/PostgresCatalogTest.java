package catalog;

import org.apache.flink.connector.jdbc.catalog.JdbcCatalogUtils;
import org.apache.flink.connector.jdbc.catalog.PostgresCatalog;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.Stream;

/**
 * @author zhangjun 欢迎关注我的公众号[大数据技术与应用实战],获取更多精彩实战内容
 * PostgresCatalog example
 */
public class PostgresCatalogTest{
	public static void main(String[] args) throws TableNotExistException{
		StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		bsEnv.enableCheckpointing(10000);
		bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(bsEnv);

		String catalogName = "mycatalog";
		String defaultDatabase = "postgres";
		String username = "postgres";
		String pwd = "postgres";
		String baseUrl = "jdbc:postgresql://localhost:5432/";

		PostgresCatalog postgresCatalog = (PostgresCatalog) JdbcCatalogUtils.createCatalog(
				catalogName,
				defaultDatabase,
				username,
				pwd,
				baseUrl);


		tEnv.registerCatalog(postgresCatalog.getName(), postgresCatalog);
		tEnv.useCatalog(postgresCatalog.getName());

		System.out.println("list databases :");
		String[] databases = tEnv.listDatabases();
		Stream.of(databases).forEach(System.out::println);
		System.out.println("--------------------- :");

		tEnv.useDatabase(defaultDatabase);
		System.out.println("list tables :");
		String[] tables = tEnv.listTables(); // 也可以使用  postgresCatalog.listTables(defaultDatabase);
		Stream.of(tables).forEach(System.out::println);

		System.out.println("list functions :");
		String[] functions = tEnv.listFunctions();
		Stream.of(functions).forEach(System.out::println);

		CatalogBaseTable catalogBaseTable = postgresCatalog.getTable(new ObjectPath(
				defaultDatabase,
				"table1"));

		TableSchema tableSchema = catalogBaseTable.getSchema();
		System.out.println("tableSchema --------------------- :");
		System.out.println(tableSchema);



		System.out.println("before insert  --------------------- :");
		List<Row> results = Lists.newArrayList(tEnv.sqlQuery("select * from table1")
		                                           .execute()
		                                           .collect());
		results.stream().forEach(System.out::println);

		tEnv.executeSql("insert into table1 values (3,'c')");

		System.out.println("after insert  --------------------- :");
		List<Row> results1 = Lists.newArrayList(tEnv.sqlQuery("select * from table1")
		                                            .execute()
		                                            .collect());
		results1.stream().forEach(System.out::println);

	}
}

package modules;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.module.hive.HiveModule;
import org.apache.flink.types.Row;

import avro.shaded.com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;

/**
 * @author zhangjun 欢迎关注我的公众号[大数据技术与应用实战],获取更多精彩实战内容
 * <p>
 * hive module测试类，在flink中使用hive的内置函数和自定义函数
 */
public class HiveModulesTest{
	public static void main(String[] args){
		StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(bsEnv);

		String name = "myhive";
		String version = "3.1.2";
		tEnv.loadModule(name, new HiveModule(version));

		System.out.println("list modules ------------------ ");
		String[] modules = tEnv.listModules();
		Arrays.stream(modules).forEach(System.out::println);

		System.out.println("list functions (包含hive函数):------------------  ");
		String[] functions = tEnv.listFunctions();
		Arrays.stream(functions).forEach(System.out::println);

		System.out.println("hive 内置函数的使用:  ------------------  ");
		String sql = "SELECT data,get_json_object(data, '$.name')  FROM (VALUES ('{\"name\":\"flink\"}'), ('{\"name\":\"hadoop\"}')) AS MyTable(data)";

		List<Row> results = Lists.newArrayList(tEnv.sqlQuery(sql)
		                                           .execute()
		                                           .collect());
		results.stream().forEach(System.out::println);

		//构造hive catalog
		String hiveCatalogName = "myhive";
		String defaultDatabase = "default";
		String hiveConfDir = "/Users/user/work/hive/conf"; // a local path
		String hiveVersion = "3.1.2";

		HiveCatalog hive = new HiveCatalog(
				hiveCatalogName,
				defaultDatabase,
				hiveConfDir,
				hiveVersion);
		tEnv.registerCatalog("myhive", hive);
		tEnv.useCatalog("myhive");
		tEnv.useDatabase(defaultDatabase);

		System.out.println("list functions (包含hive函数):------------------  ");
		String[] functions1 = tEnv.listFunctions();
		Arrays.stream(functions1).forEach(System.out::println);

		boolean b = Arrays.asList(functions1).contains("mysum");
		System.out.println("是否包含自定义函数： " + b);

		String sqlUdf = "select mysum(1,2)";
		List results1 = Lists.newArrayList(tEnv.sqlQuery(sqlUdf)
		                                       .execute()
		                                       .collect());
		System.out.println("使用自定义函数处理结果： ");
		results1.stream().forEach(System.out::println);

	}
}

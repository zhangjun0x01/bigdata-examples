package sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;


/**
 * @author zhangjun 欢迎关注我的公众号[大数据技术与应用实战],获取更多精彩实战内容
 * <p>
 * flink入门，如何使用flink的sql。
 */
public class SqlFirst{

	File tmpFile = new File("/tmp/flink_sql_first.txt");

	@Before
	public void init(){
		if (tmpFile.exists()){
			tmpFile.delete();
		}

		try {
			tmpFile.createNewFile();
			FileUtils.write(tmpFile, "peter,30");
		} catch (IOException e){
			e.printStackTrace();
		}
	}

	@Test
	public void testSQL() throws Exception{
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		//使用flink的二元组，这个时候需要自定义字段名称
		Tuple2<String,Integer> tuple2 = Tuple2.of("jack", 10);
		//构造一个Tuple的DataStream
		DataStream<Tuple2<String,Integer>> tupleStream = env.fromElements(tuple2);
//		注册到StreamTableEnvironment，并且指定对应的字段名
		tableEnv.createTemporaryView("usersTuple", tupleStream, "name,age");
		//执行一个sql 查询. 然后返回一个table对象
		Table table = tableEnv.sqlQuery("select name,age from usersTuple");
//		将table对象转成flink的DataStream，以便后续操作，我们这里将其输出
		tableEnv.toAppendStream(table, Row.class).print();

		//使用Row
		Row row = new Row(2);
		row.setField(0, "zhangsan");
		row.setField(1, 20);
		DataStream<Row> rowDataStream = env.fromElements(row);
		tableEnv.createTemporaryView("usersRow", rowDataStream, "name,age");
		Table tableRow = tableEnv.sqlQuery("select name,age from usersRow");
		tableEnv.toAppendStream(tableRow, Row.class).print();

		//使用pojo类型,不需要定义字段类型，flink会解析Pojo类型中的类型
		User user = new User();
		user.setName("Tom");
		user.setAge(20);
		DataStream<User> userDataStream = env.fromElements(user);
		tableEnv.createTemporaryView("usersPojo", userDataStream);
		Table tablePojo = tableEnv.sqlQuery("select name,age from usersPojo");
		tableEnv.toAppendStream(tablePojo, Row.class).print();

		//连接外部系统，比如文件，kafka等
		Schema schema = new Schema()
				.field("name", DataTypes.STRING())
				.field("age", DataTypes.INT());
		tableEnv.connect(new FileSystem().path(tmpFile.getPath()))
		        .withFormat(new Csv())
		        .withSchema(schema)
		        .createTemporaryTable("usersFile");
		Table tableFile = tableEnv.sqlQuery("select name,age from usersFile");
		tableEnv.toAppendStream(tableFile, Row.class).print();

		env.execute("SqlFirst");
	}

	@After
	public void end(){
		if (tmpFile.exists()){
			tmpFile.delete();
		}
	}

	public static class User{
		private String name;
		private int age;

		public String getName(){
			return name;
		}

		public void setName(String name){
			this.name = name;
		}

		public int getAge(){
			return age;
		}

		public void setAge(int age){
			this.age = age;
		}
	}
}

package connectors.sql;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import javax.annotation.Nullable;

import java.sql.Timestamp;
/**
 * @author zhangjun 欢迎关注我的公众号[大数据技术与应用实战],获取更多精彩实战内容
 * <p>
 * 流式数据以sql的形式写入hive
 */
public class StreamingWriteHive{
	public static void main(String[] args) throws Exception{
		StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		bsEnv.enableCheckpointing(10000);
		bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(bsEnv);
		DataStream<UserInfo> dataStream = bsEnv.addSource(new MySource())
		                                       .assignTimestampsAndWatermarks(
				                                       new AssignerWithPunctuatedWatermarks<UserInfo>(){
					                                       long water = 0l;
					                                       @Nullable
					                                       @Override
					                                       public Watermark checkAndGetNextWatermark(
							                                       UserInfo lastElement,
							                                       long extractedTimestamp){
						                                       return new Watermark(water);
					                                       }

					                                       @Override
					                                       public long extractTimestamp(
							                                       UserInfo element,
							                                       long recordTimestamp){
						                                       water = element.getTs().getTime();
						                                       return water;
					                                       }
				                                       });


		//构造hive catalog
		String name = "myhive";
		String defaultDatabase = "default";
		String hiveConfDir = "/Users/user/work/hive/conf"; // a local path
		String version = "3.1.2";

		HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
		tEnv.registerCatalog("myhive", hive);
		tEnv.useCatalog("myhive");
		tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
		tEnv.useDatabase("db1");

		tEnv.createTemporaryView("users", dataStream);

//      如果hive中已经存在了相应的表，则这段代码省略
//		String hiveSql = "CREATE external TABLE fs_table (\n" +
//		                 "  user_id STRING,\n" +
//		                 "  order_amount DOUBLE" +
//		                 ") partitioned by (dt string,h string,m string) " +
//		                 "stored as ORC " +
//		                 "TBLPROPERTIES (\n" +
//		                 "  'partition.time-extractor.timestamp-pattern'='$dt $h:$m:00',\n" +
//		                 "  'sink.partition-commit.delay'='0s',\n" +
//		                 "  'sink.partition-commit.trigger'='partition-time',\n" +
//		                 "  'sink.partition-commit.policy.kind'='metastore'" +
//		                 ")";
//		tEnv.executeSql(hiveSql);

		String insertSql = "insert into  fs_table SELECT userId, amount, " +
		                   " DATE_FORMAT(ts, 'yyyy-MM-dd'), DATE_FORMAT(ts, 'HH'), DATE_FORMAT(ts, 'mm') FROM users";
		tEnv.executeSql(insertSql);
	}


	public static class MySource implements SourceFunction<UserInfo>{

		String userids[] = {
				"4760858d-2bec-483c-a535-291de04b2247", "67088699-d4f4-43f2-913c-481bff8a2dc5",
				"72f7b6a8-e1a9-49b4-9a0b-770c41e01bfb", "dfa27cb6-bd94-4bc0-a90b-f7beeb9faa8b",
				"aabbaa50-72f4-495c-b3a1-70383ee9d6a4", "3218bbb9-5874-4d37-a82d-3e35e52d1702",
				"3ebfb9602ac07779||3ebfe9612a007979", "aec20d52-c2eb-4436-b121-c29ad4097f6c",
				"e7e896cd939685d7||e7e8e6c1930689d7", "a4b1e1db-55ef-4d9d-b9d2-18393c5f59ee"
		};

		@Override
		public void run(SourceContext<UserInfo> sourceContext) throws Exception{

			while (true){
				String userid = userids[(int) (Math.random() * (userids.length - 1))];
				UserInfo userInfo = new UserInfo();
				userInfo.setUserId(userid);
				userInfo.setAmount(Math.random() * 100);
				userInfo.setTs(new Timestamp(System.currentTimeMillis()));
				sourceContext.collect(userInfo);
				Thread.sleep(100);
			}
		}

		@Override
		public void cancel(){

		}
	}

	public static class UserInfo implements java.io.Serializable{
		private String userId;
		private Double amount;
		private Timestamp ts;

		public String getUserId(){
			return userId;
		}

		public void setUserId(String userId){
			this.userId = userId;
		}

		public Double getAmount(){
			return amount;
		}

		public void setAmount(Double amount){
			this.amount = amount;
		}

		public Timestamp getTs(){
			return ts;
		}

		public void setTs(Timestamp ts){
			this.ts = ts;
		}
	}
}
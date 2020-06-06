package function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * 自定义聚合函数输出
 */
public class CustomAggregateFunctionTCase{
	public static void main(String[] args) throws Exception{
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Tuple2<String,Long>> dataStream = env.addSource(new MySource());

		dataStream.keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
		          .aggregate(new CountAggregate(), new WindowResult()
		          ).print();

		env.execute();

//     使用sql实现方法
//		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//		tEnv.createTemporaryView("logs", dataStream, "user,ts,proctime.proctime");
//		Table table = tEnv.sqlQuery(
//				"select TUMBLE_START(proctime,INTERVAL '2' SECOND)  as starttime,user,count(*) from logs group by user,TUMBLE(proctime,INTERVAL '2' SECOND)");
//		DataStream result = tEnv.toAppendStream(table, Row.class);
//		result.print();
//		tEnv.execute("CustomAggregateFunction");
	}

	/**
	 * 这个是为了将聚合结果输出
	 */
	public static class WindowResult
			implements WindowFunction<Integer,Tuple3<String,Date,Integer>,Tuple,TimeWindow>{

		@Override
		public void apply(
				Tuple key,
				TimeWindow window,
				Iterable<Integer> input,
				Collector<Tuple3<String,Date,Integer>> out) throws Exception{

			String k = ((Tuple1<String>) key).f0;
			long windowStart = window.getStart();
			int result = input.iterator().next();
			out.collect(Tuple3.of(k, new Date(windowStart), result));

		}
	}

	public static class CountAggregate
			implements AggregateFunction<Tuple2<String,Long>,Integer,Integer>{

		@Override
		public Integer createAccumulator(){
			return 0;
		}

		@Override
		public Integer add(Tuple2<String,Long> value, Integer accumulator){
			return ++accumulator;
		}

		@Override
		public Integer getResult(Integer accumulator){
			return accumulator;
		}

		@Override
		public Integer merge(Integer a, Integer b){
			return a + b;
		}
	}

	public static class MySource implements SourceFunction<Tuple2<String,Long>>{

		private volatile boolean isRunning = true;

		String userids[] = {
				"4760858d-2bec-483c-a535-291de04b2247", "67088699-d4f4-43f2-913c-481bff8a2dc5",
				"72f7b6a8-e1a9-49b4-9a0b-770c41e01bfb", "dfa27cb6-bd94-4bc0-a90b-f7beeb9faa8b",
				"aabbaa50-72f4-495c-b3a1-70383ee9d6a4", "3218bbb9-5874-4d37-a82d-3e35e52d1702",
				"3ebfb9602ac07779||3ebfe9612a007979", "aec20d52-c2eb-4436-b121-c29ad4097f6c",
				"e7e896cd939685d7||e7e8e6c1930689d7", "a4b1e1db-55ef-4d9d-b9d2-18393c5f59ee"
		};

		@Override
		public void run(SourceContext<Tuple2<String,Long>> ctx) throws Exception{
			while (isRunning){
				Thread.sleep(10);
				String userid = userids[(int) (Math.random() * (userids.length - 1))];
				ctx.collect(Tuple2.of(userid, System.currentTimeMillis()));
			}
		}

		@Override
		public void cancel(){
			isRunning = false;
		}
	}

}

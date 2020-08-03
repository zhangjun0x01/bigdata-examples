package windows;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class RealTimePvUv_BitMap{
	public static void main(String[] args) throws Exception{
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<String,Integer>> dataStream = env.addSource(new MySource());

		dataStream.keyBy(0).window(TumblingProcessingTimeWindows.of(Time.days(
				1), Time.hours(-8)))
		          .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(
				          1))).aggregate(
				new MyAggregate(),
				new WindowResult()).print();
		env.execute();
	}

	public static class MyAggregate
			implements AggregateFunction<Tuple2<String,Integer>,Set<Integer>,Integer>{

		@Override
		public Set<Integer> createAccumulator(){
			return new HashSet<>();
		}

		@Override
		public Set<Integer> add(Tuple2<String,Integer> value, Set<Integer> accumulator){
			accumulator.add(value.f1);
			return accumulator;
		}

		@Override
		public Integer getResult(Set<Integer> accumulator){
			return accumulator.size();
		}

		@Override
		public Set<Integer> merge(Set<Integer> a, Set<Integer> b){
			a.addAll(b);
			return a;
		}
	}

	public static class MySource implements SourceFunction<Tuple2<String,Integer>>{

		private volatile boolean isRunning = true;
		String category[] = {"Android", "IOS", "H5"};

		@Override
		public void run(SourceContext<Tuple2<String,Integer>> ctx) throws Exception{
			while (isRunning){
				Thread.sleep(10);
				//具体是哪个端的用户
				String type = category[(int) (Math.random() * (category.length))];
				//随机生成10000以内的int类型数据作为userid
				int userid = (int) (Math.random() * 10000);
				ctx.collect(Tuple2.of(type, userid));
			}
		}

		@Override
		public void cancel(){
			isRunning = false;
		}
	}

	private static class WindowResult implements WindowFunction<Integer,Result,Tuple,TimeWindow>{
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		@Override
		public void apply(
				Tuple key,
				TimeWindow window,
				Iterable<Integer> input,
				Collector<Result> out) throws Exception{

			String type = ((Tuple1<String>) key).f0;
			int uv = input.iterator().next();
			Result result = new Result();
			result.setType(type);
			result.setUv(uv);
			result.setDateTime(simpleDateFormat.format(new Date()));
			out.collect(result);

		}
	}

	public static class Result{
		private String type;
		private int uv;
		//      截止到当前时间的时间
		private String dateTime;

		public String getDateTime(){
			return dateTime;
		}

		public void setDateTime(String dateTime){
			this.dateTime = dateTime;
		}

		public String getType(){
			return type;
		}

		public void setType(String type){
			this.type = type;
		}

		public int getUv(){
			return uv;
		}

		public void setUv(int uv){
			this.uv = uv;
		}

		@Override
		public String toString(){
			return "Result{" +
			       ", dateTime='" + dateTime + '\'' +
			       "type='" + type + '\'' +
			       ", uv=" + uv +
			       '}';
		}
	}

}

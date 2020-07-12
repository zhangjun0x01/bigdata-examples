package datastream;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.UUID;

/**
 * @author zhangjun 欢迎关注我的公众号[大数据技术与应用实战],获取更多精彩实战内容
 * <p>
 * flink 1.11 中新的水印生成器
 */
public class WatermarkTest{
	public static void main(String[] args){

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		//设置周期性水印的间隔
		env.getConfig().setAutoWatermarkInterval(5000L);
		DataStream<Tuple2<String,Long>> dataStream = env.addSource(new MySource());

		DataStream<Tuple2<String,Long>> withTimestampsAndWatermarks = dataStream.assignTimestampsAndWatermarks(
				new WatermarkStrategy<Tuple2<String,Long>>(){
					@Override
					public WatermarkGenerator<Tuple2<String,Long>> createWatermarkGenerator(
							WatermarkGeneratorSupplier.Context context){
						return new WatermarkGenerator<Tuple2<String,Long>>(){
							private long maxTimestamp;
							private long delay = 3000;

							@Override
							public void onEvent(
									Tuple2<String,Long> event,
									long eventTimestamp,
									WatermarkOutput output){
								maxTimestamp = Math.max(maxTimestamp, event.f1);
							}

							@Override
							public void onPeriodicEmit(WatermarkOutput output){
								output.emitWatermark(new Watermark(maxTimestamp - delay));
							}
						};
					}
				});

//使用内置的水印生成器
//		DataStream<Tuple2<String,Long>> withTimestampsAndWatermarks = dataStream.assignTimestampsAndWatermarks(
//				WatermarkStrategy
//						.<Tuple2<String,Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
//						.withTimestampAssigner((event, timestamp)->event.f1));

		withTimestampsAndWatermarks.process(new ProcessFunction<Tuple2<String,Long>,Object>(){

			@Override
			public void processElement(
					Tuple2<String,Long> value, Context ctx, Collector<Object> out) throws Exception{
				long w = ctx.timerService().currentWatermark();
				System.out.println(" 水印 ： " + w + "  water  date " + new Date(w) + " now " +
				                   new Date(value.f1));
			}
		});

		try {
			env.execute();
		} catch (Exception e){
			e.printStackTrace();
		}
	}

	public static class MySource implements SourceFunction<Tuple2<String,Long>>{
		private volatile boolean isRunning = true;

		@Override
		public void run(SourceContext<Tuple2<String,Long>> ctx) throws Exception{
			while (isRunning){
				Thread.sleep(1000);
				//订单id
				String orderid = UUID.randomUUID().toString();
				//订单完成时间
				long orderFinishTime = System.currentTimeMillis();
				ctx.collect(Tuple2.of(orderid, orderFinishTime));
			}
		}

		@Override
		public void cancel(){
			isRunning = false;
		}
	}
}

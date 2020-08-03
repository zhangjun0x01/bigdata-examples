package cep;/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.Timestamp;
import java.util.Random;
import java.util.UUID;

/**
 * 使用广播实现动态的配置更新
 */
public class WebMonitorAlertDynamicConf1{

	private static final Logger LOG = LoggerFactory.getLogger(WebMonitorAlertDynamicConf1.class);

	public static void main(String[] args) throws Exception{
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Tuple5<String,Long,Integer,Integer,String>> ds = env.addSource(new MySource())
		                                                               .assignTimestampsAndWatermarks(
				                                                               new AssignerWithPunctuatedWatermarks<Tuple5<String,Long,Integer,Integer,String>>(){
					                                                               @Override
					                                                               public long extractTimestamp(
							                                                               Tuple5<String,Long,Integer,Integer,String> element,
							                                                               long previousElementTimestamp){
						                                                               return element.f1;
					                                                               }

					                                                               @Nullable
					                                                               @Override
					                                                               public Watermark checkAndGetNextWatermark(
							                                                               Tuple5<String,Long,Integer,Integer,String> lastElement,
							                                                               long extractedTimestamp){
						                                                               return new Watermark(
								                                                               lastElement.f1);
					                                                               }
				                                                               });
		StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
		tenv.registerDataStream(
				"log",
				ds,
				"traceid,timestamp,status,restime,type,proctime.proctime");

		String sql =
				"select type,pv,errorcount,round(CAST(errorcount AS DOUBLE)/pv,2) as errorRate," +
				"(starttime + interval '8' hour ) as stime," +
				"(endtime + interval '8' hour ) as etime  " +
				"from (select type,count(*) as pv," +
				"sum(case when status = 200 then 0 else 1 end) as errorcount, " +
				"TUMBLE_START(proctime,INTERVAL '1' SECOND)  as starttime," +
				"TUMBLE_END(proctime,INTERVAL '1' SECOND)  as endtime  " +
				"from log  group by type,TUMBLE(proctime,INTERVAL '1' SECOND) )";

		Table table = tenv.sqlQuery(sql);
		DataStream<Result> dataStream = tenv.toAppendStream(table, Result.class);

		MapStateDescriptor<String,Long> confDescriptor = new MapStateDescriptor<>(
				"config-keywords",
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.LONG_TYPE_INFO);

		DataStream confStream = env.addSource(new BroadcastSource());

		BroadcastStream<Integer> broadcastStream = confStream.broadcast(confDescriptor);
// .connect(broadcastStream).process(new MyKeyBroad());

		DataStream dd = dataStream.keyBy("type")
//		                          .connect(broadcastStream).process(new MyKeyBroad());

		     .process(new KeyedProcessFunction<Tuple,Result,Object>(){
			@Override
			public void processElement(
					Result result, Context ctx, Collector<Object> out) throws Exception{
				System.out.println(
						result.getType() + "  " + ctx.timerService().currentWatermark());
				ctx.timerService().registerEventTimeTimer(result.getEtime().getTime() + 5000L);
			}

			@Override
			public void onTimer(
					long timestamp,
					OnTimerContext ctx,
					Collector<Object> out) throws Exception{
				System.out.println(111111L);
			}
		});

//		dataStream.process(new MyKeyBroad());

//		DataStream resultStream = dataStream.connect(broadcastStream)
//		                                    .process(new BroadcastProcessFunction<Result,Integer,Result>(){
//			                                    @Override
//			                                    public void processElement(
//					                                    Result element,
//					                                    ReadOnlyContext ctx,
//					                                    Collector<Result> out) throws Exception{
//				                                    Long v = ctx.getBroadcastState(confDescriptor)
//				                                                .get("value");
//				                                    if (v != null && element.getErrorcount() > v){
//					                                    LOG.info("收到了一个大于阈值{}的结果{}.", v, element);
//					                                    out.collect(element);
//				                                    }
//			                                    }
//
//			                                    @Override
//			                                    public void processBroadcastElement(
//					                                    Integer value,
//					                                    Context ctx,
//					                                    Collector<Result> out) throws Exception{
//				                                    ctx.getBroadcastState(confDescriptor)
//				                                       .put("value", value.longValue());
//
//			                                    }
//		                                    });

		env.execute("FlinkDynamicConf");
	}

	public static class MyKeyBroad
			extends KeyedBroadcastProcessFunction<String,Result,Integer,Result>{

		@Override
		public void processElement(
				Result result, ReadOnlyContext ctx, Collector<Result> out) throws Exception{
			System.out.println(
					"processBroadcastElement result etime " + result + "   watermark is  " +
					ctx.currentWatermark());

			out.collect(result);
		}

		@Override
		public void processBroadcastElement(
				Integer value, Context ctx, Collector<Result> out) throws Exception{
//			System.out.println(
//					"processBroadcastElement result etime " + value + "   watermark is  " +
//					ctx.currentWatermark());
		}
	}

	public static class BroadcastSource implements SourceFunction<Integer>{

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception{
			while (true){
				Thread.sleep(3000);
				ctx.collect(randInt(15, 20));
			}
		}

		/**
		 * 生成指定范围内的随机数
		 *
		 * @param min
		 * @param max
		 * @return
		 */
		private int randInt(int min, int max){
			Random rand = new Random();
			int randomNum = rand.nextInt((max - min) + 1) + min;
			return randomNum;
		}

		@Override
		public void cancel(){

		}
	}

	public static class MySource
			implements SourceFunction<Tuple5<String,Long,Integer,Integer,String>>{

		static int status[] = {200, 404, 500, 501, 301};

		@Override
		public void run(SourceContext<Tuple5<String,Long,Integer,Integer,String>> sourceContext) throws Exception{
			while (true){
				Thread.sleep((int) (Math.random() * 100));
				// traceid,timestamp,status,response time
				String type = "flink";
				Tuple5 log = Tuple5.of(
						UUID.randomUUID().toString(),
						System.currentTimeMillis(),
						status[(int) (Math.random() * 4)],
						(int) (Math.random() * 100),
						type);

				sourceContext.collect(log);
			}
		}

		@Override
		public void cancel(){

		}
	}

	public static class Result{
		private long pv;
		private int errorcount;
		private double errorRate;
		private Timestamp stime;
		private Timestamp etime;
		private String type;

		public String getType(){
			return type;
		}

		public void setType(String type){
			this.type = type;
		}

		public long getPv(){
			return pv;
		}

		public void setPv(long pv){
			this.pv = pv;
		}

		public int getErrorcount(){
			return errorcount;
		}

		public void setErrorcount(int errorcount){
			this.errorcount = errorcount;
		}

		public double getErrorRate(){
			return errorRate;
		}

		public void setErrorRate(double errorRate){
			this.errorRate = errorRate;
		}

		public Timestamp getStime(){
			return stime;
		}

		public void setStime(Timestamp stime){
			this.stime = stime;
		}

		public Timestamp getEtime(){
			return etime;
		}

		public void setEtime(Timestamp etime){
			this.etime = etime;
		}

		@Override
		public String toString(){
			return "Result{" +
			       "pv=" + pv +
			       ", errorcount=" + errorcount +
			       ", errorRate=" + errorRate +
			       ", stime=" + stime +
			       ", etime=" + etime +
			       '}';
		}
	}

}

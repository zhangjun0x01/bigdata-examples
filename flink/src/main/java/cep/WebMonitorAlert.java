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

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 通过使用flink cep进行网站的监控报警和恢复通知
 */
public class WebMonitorAlert{

	public static void main(String[] args) throws Exception{
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream ds = env.addSource(new MySource());
		StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
		tenv.registerDataStream(
				"log",
				ds,
				"traceid,timestamp,status,restime,proctime.proctime");

		String sql = "select pv,errorcount,round(CAST(errorcount AS DOUBLE)/pv,2) as errorRate," +
		             "(starttime + interval '8' hour ) as stime," +
		             "(endtime + interval '8' hour ) as etime  " +
		             "from (select count(*) as pv," +
		             "sum(case when status = 200 then 0 else 1 end) as errorcount, " +
		             "TUMBLE_START(proctime,INTERVAL '1' SECOND)  as starttime," +
		             "TUMBLE_END(proctime,INTERVAL '1' SECOND)  as endtime  " +
		             "from log  group by TUMBLE(proctime,INTERVAL '1' SECOND) )";

		Table table = tenv.sqlQuery(sql);
		DataStream<Result> ds1 = tenv.toAppendStream(table, Result.class);

		ds1.print();

		Pattern pattern = Pattern.<Result>begin("alert").where(new IterativeCondition<Result>(){
			@Override
			public boolean filter(
					Result i, Context<Result> context) throws Exception{
				return i.getErrorRate() > 0.7D;
			}
		}).times(3).consecutive().followedBy("recovery").where(new IterativeCondition<Result>(){
			@Override
			public boolean filter(
					Result i,
					Context<Result> context) throws Exception{
				return i.getErrorRate() <= 0.7D;
			}
		}).optional();

		DataStream<Map<String,List<Result>>> alertStream = org.apache.flink.cep.CEP.pattern(
				ds1,
				pattern).select(new PatternSelectFunction<Result,Map<String,List<Result>>>(){
			@Override
			public Map<String,List<Result>> select(Map<String,List<Result>> map) throws Exception{
				List<Result> alertList = map.get("alert");
				List<Result> recoveryList = map.get("recovery");

				if (recoveryList != null){
					System.out.print("接受到了报警恢复的信息，报警信息如下：");
					System.out.print(alertList);
					System.out.print("  对应的恢复信息：");
					System.out.println(recoveryList);
				} else {
					System.out.print("收到了报警信息 ");
					System.out.print(alertList);
				}

				return map;
			}
		});

		env.execute("Flink CEP web alert");
	}

	public static class MySource implements SourceFunction<Tuple4<String,Long,Integer,Integer>>{

		static int status[] = {200, 404, 500, 501, 301};

		@Override
		public void run(SourceContext<Tuple4<String,Long,Integer,Integer>> sourceContext) throws Exception{
			while (true){
				Thread.sleep((int) (Math.random() * 100));
				// traceid,timestamp,status,response time

				Tuple4 log = Tuple4.of(
						UUID.randomUUID().toString(),
						System.currentTimeMillis(),
						status[(int) (Math.random() * 4)],
						(int) (Math.random() * 100));

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

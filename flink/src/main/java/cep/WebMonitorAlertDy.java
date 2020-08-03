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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.List;
import java.util.Map;

/**
 * 通过使用flink cep进行网站的监控报警和恢复通知
 */
public class WebMonitorAlertDy{


	public static void main(String[] args) throws Exception{

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000);
		DataStream ds = env.addSource(new MySource());


		Pattern pattern = Pattern.<Tuple2<String,String>>begin("index").where(new IterativeCondition<Tuple2<String,String>>(){
			@Override
			public boolean filter(
					Tuple2<String,String> i, Context<Tuple2<String,String>> context) throws Exception{
				return i.getField(1).equals("index");
			}
		}).followedBy("list").where(new IterativeCondition<Tuple2<String,String>>(){
			@Override
			public boolean filter(
					Tuple2<String,String> i,
					Context<Tuple2<String,String>> context) throws Exception{
				return i.getField(1).equals("list");
			}
		}).optional();


		DataStream<Map<String,List<Tuple2<String,String>>>> alertStream = org.apache.flink.cep.CEP.pattern(
				ds,
				pattern).select(new PatternSelectFunction<Tuple2<String,String>,Map<String,List<Integer>>>(){
			@Override
			public Map<String,List<Integer>> select(Map<String,List<Tuple2<String,String>>> map) throws Exception{

				List<Tuple2<String,String>>  index = map.get("index");
				List<Tuple2<String,String>>  list = map.get("list");

				System.out.print("index "+index);
				System.out.println("  list  "+list);
				return null;
			}
		});

		env.execute("Flink CEP web alert");
	}

	public static class MySource implements SourceFunction<Tuple2<String,String>>{

		String pages[] = {"index","list", "detail", "order", "pay"};

		String userids[] = {"4760858d-2bec-483c-a535-291de04b2247", "67088699-d4f4-43f2-913c-481bff8a2dc5", "72f7b6a8-e1a9-49b4-9a0b-770c41e01bfb", "dfa27cb6-bd94-4bc0-a90b-f7beeb9faa8b", "aabbaa50-72f4-495c-b3a1-70383ee9d6a4", "3218bbb9-5874-4d37-a82d-3e35e52d1702", "3ebfb9602ac07779||3ebfe9612a007979", "aec20d52-c2eb-4436-b121-c29ad4097f6c", "e7e896cd939685d7||e7e8e6c1930689d7", "a4b1e1db-55ef-4d9d-b9d2-18393c5f59ee", "bae0133a-6097-4230-81e6-e79e330d6dc0", "d34b7863-ce57-4972-9013-7190a73df9c2", "0f899875-cb85-4c93-ac0a-aa5b7c24f90b", "fa848a76-1720-4bad-8d42-363582e350e1", "d99f76c5-4205-42a9-bf01-b77d72acfe89", "40b55d37-88a6-4227-8adc-eb99cf7223f1", "2e4684cb-0ede-4463-907f-b0ef92ed6304", "a9fcf5a4-9780-4665-a730-4c161da7e4a5", "1e2356ee-906d-49e0-8241-ce276e54b260", "bd821b42-c915-4bcb-a8e4-e7c1d086f1a2", "6b419af4-04cc-4515-8e43-e336f58838f3", "719c1c49-b56e-43e3-ae5a-7ab1d7d5aa67", "38e0031b-1a59-42c3-bfd7-c0b6376672f0", "841f8aee-5d54-47f9-aff4-ddc389cc5177", "dd8e0127-6e69-4455-9ff8-7e3fa7b0ab7f", "7e3a9888-5872-44d3-8d9e-f29264a5d850", "ee357668-f1dc-47bd-a2c8-bc0418f44dee", "ff662564-c409-4dd6-a25c-9e5aff150a19", "6ac6943f-b349-465d-aeef-d27a68eece00", "e93a83ac-ad44-4453-8816-694dca7e08b9", "14796bf5-a1db-4eae-8f01-3559d11d4219", "615b477a-4376-4076-911e-72e82572cab1", "867909032116851||8679e9012106891", "358811071303117||3588e1011303197", "905e0993-408a-43f0-8f3b-559e4246dc02", "04d1ca7e-2c47-41c8-8c0a-0361a654db1a", "a377c74d-3995-47e4-8417-a38839d8afe6", "a0cc47f19b76b521||a0cce7f19b06b921", "9fee2765-0373-4bae-8f5a-fd206b2e03c1", "de0f472b-f2a0-4fa1-b79b-b0a8c42e5454", "7dd2b7b2-b62f-464b-b33c-adb5da4a9c3d", "fb8d4fbc-cee6-465d-acc5-31644a43bc51", "15688f77-6232-4f51-bbff-0cff02a4a51b", "5311fa69-4fa5-4d7e-a84e-408a0e300b9b", "f02f5792-800a-4a52-abab-a5c91d2f74da", "10fc49c6-be55-4bf6-ae63-275bccd86c49", "7bca4201-5c52-4b1b-9c65-c47d9d041b4b", "b2403d70-8ba4-454b-99a8-4ea357b905a4", "df051af8-ac97-418d-b489-e33126937c4c", "c8a445d8-3e0f-40d1-88a5-be0cf67b0d5e", "6a7a4533-afb5-4916-b891-5275e1c4632d", "4181e0ff-57d8-4b60-b576-7b9c2694e3e8", "841e4907-67a7-4e74-b8be-3da0538014fa", "6c96ecb388c5e01||6c96ecb18805e91", "95433f82-10fd-4d2b-9afd-5e32e8b104b6", "932ab2b8-d235-45bd-baf4-d10917e50e00", "9bfe0609-c4f3-4db9-8280-d16c47f112e3", "f2b100494eda48db||f2b1e0414e0a49db", "100cca6c-11ca-4c64-9148-0c65585b7a2f", "865582034108598||8655e2014108598", "e1b31de3-ee29-4060-9a06-a2b8807ef78c", "c71f4fe8-fb98-435d-b47b-7eec1391f28b", "9bd097bc-aa42-419d-b27f-1f805364c23e", "93f1bbe9-be10-41a2-aa76-ad57939e4a60", "53095209-0a58-48f2-b644-823e54ef3966", "12bee639-3902-4a0b-a227-5fd29474d8b5", "dd4994ee-98dc-4b45-a5af-c8c3cf4042c6", "c0f9d530-0b4b-46bf-96dc-4fe9b737e640", "04d4b7a0-d1d7-472a-88ef-4e693cc40224", "6ef99780-d8a5-4f11-b07a-a5e73444347c", "fc536ca3-5334-4836-a4a4-151c35382070", "c4ed892d-575b-40b6-9612-481c723bb087", "80e79e50-c343-4a33-9f7b-d7d813682045", "0e69d7eb-b42b-4f1a-99cc-3bac16a73953", "5389c9f0-c1a5-4092-8d1b-06c768178d5b", "696bf70e-f313-4fc9-b1f9-49934c9b9526", "c28537dd-9cbd-40f6-849d-df5f4960d0c5", "f7d36cb3-b023-40d1-99af-288fd3ad05cc", "5032a203-d7f1-44e9-833f-dc317f788669", "845ef4b3-2d85-4608-90e8-09712f402616", "64d08fac-9398-466c-af47-a2db6a68da1f", "c551f171-c51b-4acd-a226-c4009db1b7e2", "9237d9d3-fda0-4e6f-968b-a7996d18f28e", "0ea04ca2-da36-4260-8765-6dd45f3d892e", "1523f2d4-b1bc-4303-9782-9bc961e90c3a", "ef14cb2e-a432-4ea1-a4ef-ca5213e078d3", "b1bb083b-7e9b-41cf-8fc1-5669d63a47c5", "35734607256684||3573e6012506890", "03411593-18cd-4a3b-89a9-b61ed3ee319d", "ee620a49-274c-47f2-bd01-acb1f685d049", "c148b5de-217e-4807-ad80-17739c27d2a2", "89cd93a0-8438-4479-9492-1a93c2c74549", "3604e8f5-e2bc-43cb-823b-a057d08af825", "85f196a1-e84c-4523-8312-6bc4c7fe2391", "c28b2542-7b46-477d-abfd-5ed1176e8d03", "3201376a-d1c7-4ddb-8835-1d0b960bbc52", "9ffbd17a-6544-4723-89e5-7b651ef2e5ba", "2be8741c-306d-43ed-97d7-fe3d624282f0", "a14ebc87-466f-4a62-a480-14cb040f9048", "14e261d4-536c-499e-b078-f9bf841f464d"};


		@Override
		public void run(SourceContext<Tuple2<String,String>> sourceContext) throws Exception{
			while (true){
				Thread.sleep((int) (Math.random() * 100));
				// userid,page

				Tuple2 log = Tuple2.of(
						userids[(int) (Math.random() * userids.length-1)],
						pages[(int) (Math.random() * 4)]);

				sourceContext.collect(log);
			}
		}

		@Override
		public void cancel(){

		}
	}


}

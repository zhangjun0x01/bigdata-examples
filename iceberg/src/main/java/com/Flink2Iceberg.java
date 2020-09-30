/*
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

package com;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zhangjun 欢迎关注我的公众号[大数据技术与应用实战],获取更多精彩实战内容
 * flink 流式数据写入iceberg
 *
 */
public class Flink2Iceberg{

	public static void main(String[] args) throws Exception{
		StreamExecutionEnvironment env =
				StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.enableCheckpointing(10000);
		StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
		tenv.executeSql("CREATE CATALOG iceberg WITH (\n" +
		                "  'type'='iceberg',\n" +
		                "  'catalog-type'='hive'," +
		                //"  'hive-site-path'='hdfs://localhost/data/flink/conf/hive-site.xml'" +
		                "  'hive-site-path'='/Users/user/work/hive/conf/hive-site.xml'" +
		                ")");

		tenv.useCatalog("iceberg");
		tenv.executeSql("CREATE DATABASE iceberg_db");
		tenv.useDatabase("iceberg_db");

		tenv.executeSql("CREATE TABLE sourceTable (\n" +
		                " userid int,\n" +
		                " f_random_str STRING\n" +
		                ") WITH (\n" +
		                " 'connector' = 'datagen',\n" +
		                " 'rows-per-second'='100',\n" +
		                " 'fields.userid.kind'='random',\n" +
		                " 'fields.userid.min'='1',\n" +
		                " 'fields.userid.max'='100',\n" +
		                "'fields.f_random_str.length'='10'\n" +
		                ")");

		tenv.executeSql(
				"insert into iceberg.iceberg_db.iceberg_001 select * from iceberg.iceberg_db.sourceTable");
	}
}

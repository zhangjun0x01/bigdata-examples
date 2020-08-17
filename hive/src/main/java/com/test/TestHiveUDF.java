package com.test;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;


/**
 * @author zhangjun 欢迎关注我的公众号[大数据技术与应用实战],获取更多精彩实战内容
 * hive udf
 */
public class TestHiveUDF extends UDF{

	public IntWritable evaluate(IntWritable i,IntWritable j){
		return new IntWritable(i.get() + j.get());
	}

}
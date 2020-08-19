package connectors.redis;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.net.InetSocketAddress;
import java.util.HashSet;

/**
 * @author zhangjun 欢迎关注我的公众号[大数据技术与应用实战],获取更多精彩实战内容
 * <p>
 * write redis
 */
public class RedisSinkTest{
	public static void main(String[] args) throws Exception{
		StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();

		//user,subject,score
		Tuple3<String,String,String> tuple = Tuple3.of("tom", "math", "100");
		DataStream<Tuple3<String,String,String>> dataStream = bsEnv.fromElements(tuple);

		FlinkJedisConfigBase conf = getRedisConfig();
		RedisSink redisSink = new RedisSink<>(conf, new RedisExampleMapper());

		dataStream.addSink(redisSink);
		bsEnv.execute("RedisSinkTest");
	}




	/**
	 * 获取redis单机的配置
	 * @return
	 */
	public static FlinkJedisPoolConfig getRedisConfig(){
		FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("10.160.85.185")
		                                                              // 可选 .setPassword("1234")
		                                                              .setPort(6379)
		                                                              .build();
		return conf;
	}


	/**
	 * 获取redis集群的配置
	 * @return
	 */
	public static FlinkJedisClusterConfig getRedisClusterConfig(){
		InetSocketAddress host0 = new InetSocketAddress("host1", 6379);
		InetSocketAddress host1 = new InetSocketAddress("host2", 6379);
		InetSocketAddress host2 = new InetSocketAddress("host3", 6379);

		HashSet<InetSocketAddress> set = new HashSet<>();
		set.add(host0);
		set.add(host1);
		set.add(host2);

		FlinkJedisClusterConfig config = new FlinkJedisClusterConfig.Builder().setNodes(set)
		                                                                      .build();
		return config;
	}


	public static class RedisExampleMapper implements RedisMapper<Tuple3<String,String,String>>{

		@Override
		public RedisCommandDescription getCommandDescription(){
			return new RedisCommandDescription(RedisCommand.HSET, "HASH_NAME");
		}

		@Override
		public String getKeyFromData(Tuple3<String,String,String> data){
			return data.f0;
		}

		@Override
		public String getValueFromData(Tuple3<String,String,String> data){
			return data.f2;
		}

	}

}

package sql.function.tablefunction;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author zhangjun 欢迎关注我的公众号[大数据技术与应用实战],获取更多精彩实战内容
 * <p>
 * 使用tablefuntion实现mysql维表功能
 */
public class MySQLTableFunction extends TableFunction<Row>{
	private static final Logger LOG = LoggerFactory.getLogger(MySQLTableFunction.class);
	//从外部传进来的参数,必填字段
	private String url;
	private String username;
	private String password;
	private String tableName;
	//可能是联合主键
	private String[] primaryKeys;
	private RowTypeInfo rowTypeInfo;

	//    选填字段
	private int cacheSize;
	private int cacheTTLMs;

	private static final int cacheSizeDefaultValue = 1000;
	private static final int cacheTTLMsDefaultValue = 1000 * 60 * 60;

	Connection conn = null;
	PreparedStatement ps = null;
	ResultSet rs = null;

	String[] fileFields = null;
	public LoadingCache<Object[],List<Row>> funnelCache = null;

	private List<Row> getRowData(Object[] primaryKeys) throws SQLException{
		int fieldCount = fileFields.length;

		for (int i = 0; i < primaryKeys.length; i++){
			ps.setObject(i + 1, primaryKeys[i]);
		}

		rs = ps.executeQuery();
		List<Row> rowList = new ArrayList<>();
		while (rs.next()){
			Row row = new Row(fieldCount);
			for (int i = 0; i < fieldCount; i++){
				row.setField(i, rs.getObject(i + 1));
			}
			rowList.add(row);
		}
		return rowList;
	}

	public MySQLTableFunction(
			RowTypeInfo rowTypeInfo,
			String url,
			String username,
			String password,
			String tableName,
			String[] primaryKey){
		this(
				rowTypeInfo,
				url,
				username,
				password,
				tableName,
				primaryKey,
				cacheSizeDefaultValue,
				cacheTTLMsDefaultValue);
	}

	public MySQLTableFunction(
			RowTypeInfo rowTypeInfo,
			String url,
			String username,
			String password,
			String tableName,
			String[] primaryKey,
			int cacheSize,
			int cacheTTLMs){
		this.rowTypeInfo = rowTypeInfo;
		this.url = url;
		this.username = username;
		this.password = password;
		this.tableName = tableName;
		this.primaryKeys = primaryKey;
		this.cacheSize = cacheSize;
		this.cacheTTLMs = cacheTTLMs;
	}

	public void eval(Object... primaryKeys) throws ExecutionException{
		List<Row> rowList = funnelCache.get(primaryKeys);
		for (int i = 0; i < rowList.size(); i++){
			collect(rowList.get(i));
		}

	}

	@Override
	public TypeInformation<Row> getResultType(){
		return org.apache.flink.api.common.typeinfo.Types.ROW_NAMED(
				rowTypeInfo.getFieldNames(),
				rowTypeInfo.getFieldTypes());
	}

	@Override
	public void open(FunctionContext context) throws Exception{
		Class.forName("com.mysql.jdbc.Driver");
		conn = DriverManager.getConnection(url, username, password);
		fileFields = rowTypeInfo.getFieldNames();

		String fields = StringUtils.join(fileFields, ",");
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT ").append(fields).append(" FROM ").append(tableName).append(" where ");

		int primaryLen = primaryKeys.length;
		for (int i = 0; i < primaryLen; i++){
			sql.append(primaryKeys[i]).append(" =  ? ");
			if (i != primaryLen - 1){
				sql.append(" and ");
			}
		}
		LOG.info("mysql open , the sql is {}", sql);
		ps = conn.prepareStatement(sql.toString());

		funnelCache = CacheBuilder.newBuilder()
		                          .maximumSize(cacheSize)
		                          .expireAfterAccess(cacheTTLMs, TimeUnit.MILLISECONDS)
		                          .build(new CacheLoader<Object[],List<Row>>(){
			                          @Override
			                          public List<Row> load(Object[] primaryKey) throws Exception{
				                          return getRowData(primaryKey);
			                          }
		                          });
	}

	@Override
	public void close() throws Exception{
		rs.close();
		ps.close();
		conn.close();
	}
}
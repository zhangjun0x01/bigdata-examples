package example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * @author zhangjun 欢迎关注我的公众号[大数据技术与应用实战],获取更多精彩实战内容
 * <p>
 * 实时计算pv值，然后实时写入mysql
 */
public class PV2mysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        bsEnv.enableCheckpointing(100000);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(bsEnv);

        String sourceSql = "CREATE TABLE datagen (\n" +
                " userid int,\n" +
                " proctime as PROCTIME()\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='100',\n" +
                " 'fields.userid.kind'='random',\n" +
                " 'fields.userid.min'='1',\n" +
                " 'fields.userid.max'='100'\n" +
                ")";


        tEnv.executeSql(sourceSql);

        String mysqlsql = "CREATE TABLE pv (\n" +
                "  day_str STRING,\n" +
                "  pv bigINT,\n" +
                "  PRIMARY KEY (day_str) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'username' = 'root',\n" +
                "   'password' = 'root',\n" +
                "   'url' = 'jdbc:mysql://localhost:3306/test',\n" +
                "   'table-name' = 'pv'\n" +
                ")";

        tEnv.executeSql(mysqlsql);

        tEnv.executeSql("insert into pv SELECT DATE_FORMAT(proctime, 'yyyy-MM-dd') as day_str, count(*) \n" +
                "FROM datagen \n" +
                "GROUP BY DATE_FORMAT(proctime, 'yyyy-MM-dd')");

        bsEnv.execute();
    }
}

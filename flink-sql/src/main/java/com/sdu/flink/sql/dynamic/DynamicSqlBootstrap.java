package com.sdu.flink.sql.dynamic;

import com.sdu.flink.utils.UserActionEntry;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DStreamTableSourceImpl;
import org.apache.flink.table.api.DTableEnvironmentUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author hanhan.zhang
 * */
public class DynamicSqlBootstrap {

  public static void main(String[] args) throws Exception {

    String[] actions = new String[] {"Login", "Buy", "Pay", "Exit"};

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // 设置watermark间隔
    env.getConfig().setAutoWatermarkInterval(1000);

    StreamTableEnvironment tableEnv = DTableEnvironmentUtils.create(env);

    // 数据源
    TableSchema tableSchema = TableSchema.builder()
        .field("uid", DataTypes.STRING())
        .field("age", DataTypes.INT())
        .field("isForeigners", DataTypes.BOOLEAN())
        .field("action", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .build();

    DStreamTableSourceImpl<UserActionEntry> tableSource = new DStreamTableSourceImpl<>(
        new DUserActionSourceFunction(actions), tableSchema, "", 2000);
    tableEnv.registerTableSource("user_action", tableSource);

    // 注册UDF
    tableEnv.registerFunction("ADD_PREFIX", new SimpleScalarFunction());

    // 注册输出
    tableEnv.registerTableSink("user_behavior", new SimplePrintDTableSink(tableSchema));


    String sqlText = "INSERT INTO user_behavior SELECT ADD_PREFIX(uid) as userId, age + 1 as age, isForeigners, action FROM user_action where action <> 'Login'";

    tableEnv.sqlUpdate(sqlText);

    tableEnv.execute("DynamicSqlBootstrap");

  }

}

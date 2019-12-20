package com.sdu.flink.dsql;

import com.sdu.flink.dsql.functions.TextFormatScalarFunction;
import com.sdu.flink.dsql.functions.UserActionStreamSourceFunction;
import com.sdu.flink.dsql.sink.ConsoleOutputAppendTableSink;
import com.sdu.flink.utils.UserActionEntry;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DStreamTableSourceImpl;
import org.apache.flink.table.api.DTableEnvironmentUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class DSqlBootstrap {

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setAutoWatermarkInterval(1000);
    StreamTableEnvironment tableEnv = DTableEnvironmentUtils.create(env);

    // 注册数据源
    TableSchema inputTableSchema = TableSchema.builder()
        .field("uid", DataTypes.STRING())
        .field("uname", DataTypes.STRING())
        .field("sex", DataTypes.STRING())
        .field("age", DataTypes.INT())
        .field("action", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .build();
    DStreamTableSourceImpl<UserActionEntry> tableSource = new DStreamTableSourceImpl<>(
        new UserActionStreamSourceFunction(), inputTableSchema, "", 2000);
    tableEnv.registerTableSource("user_action", tableSource);

    // 注册UDF
    tableEnv.registerFunction("TEXT_FORMAT", new TextFormatScalarFunction());

    // 注册输出
    TableSchema outputTableSchema = TableSchema.builder()
        .field("uid", DataTypes.STRING())
        .field("uname", DataTypes.STRING())
        .field("sex", DataTypes.STRING())
        .field("age", DataTypes.INT())
        .field("action", DataTypes.STRING())
        .build();
    tableEnv.registerTableSink("user_behavior", new ConsoleOutputAppendTableSink(outputTableSchema));

    String sqlText = "INSERT INTO user_behavior "
        + "SELECT "
        + "TEXT_FORMAT(uid) as uid, uname, sex, age + 1 as age, action "
        + "FROM user_action "
        + "WHERE action <> '登录'";

    tableEnv.sqlUpdate(sqlText);

    tableEnv.execute("DynamicSqlBootstrap");
  }

}

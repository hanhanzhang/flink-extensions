package com.sdu.flink.table;

import com.sdu.flink.utils.JsonUtils;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.updater.DColumnStatement;
import org.apache.flink.table.updater.DTableStatement;

public class TableStatementBuilder {

  public static void main(String[] args) {

    // 输入表
    DTableStatement inputTable = new DTableStatement();
    inputTable.setTableName("user_action");

    List<DColumnStatement> inputTableColumns = new ArrayList<>();
    inputTableColumns.add(new DColumnStatement("uid", "String"));
    inputTableColumns.add(new DColumnStatement("age", "Integer"));
    inputTableColumns.add(new DColumnStatement("isForeigners", "Boolean"));
    inputTableColumns.add(new DColumnStatement("action", "String"));
    inputTableColumns.add(new DColumnStatement("timestamp", "Long"));
    inputTable.setColumns(inputTableColumns);

    // 输出表
    DTableStatement sinkTable = new DTableStatement();
    sinkTable.setTableName("user_behavior");
    List<DColumnStatement> sinkTableColumns = new ArrayList<>();
    sinkTableColumns.add(new DColumnStatement("uid", "String"));
    sinkTableColumns.add(new DColumnStatement("age", "Integer"));
    sinkTableColumns.add(new DColumnStatement("isForeigners", "Boolean"));
    sinkTableColumns.add(new DColumnStatement("action", "String"));
    sinkTableColumns.add(new DColumnStatement("timestamp", "Long"));
    sinkTable.setColumns(sinkTableColumns);

    List<DTableStatement> tableStatements = new ArrayList<>();
    tableStatements.add(inputTable);
    tableStatements.add(sinkTable);

    System.out.println(JsonUtils.toJson(tableStatements));


  }

}

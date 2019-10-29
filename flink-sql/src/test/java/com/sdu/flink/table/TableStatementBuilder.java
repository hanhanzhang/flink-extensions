package com.sdu.flink.table;

import com.sdu.flink.utils.JsonUtils;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.updater.ColumnStatement;
import org.apache.flink.table.updater.TableStatement;

public class TableStatementBuilder {

  public static void main(String[] args) {

    // 输入表
    TableStatement inputTable = new TableStatement();
    inputTable.setTableName("user_action");

    List<ColumnStatement> inputTableColumns = new ArrayList<>();
    inputTableColumns.add(new ColumnStatement("uid", "String"));
    inputTableColumns.add(new ColumnStatement("age", "Integer"));
    inputTableColumns.add(new ColumnStatement("isForeigners", "Boolean"));
    inputTableColumns.add(new ColumnStatement("action", "String"));
    inputTableColumns.add(new ColumnStatement("timestamp", "Long"));
    inputTable.setColumns(inputTableColumns);

    // 输出表
    TableStatement sinkTable = new TableStatement();
    sinkTable.setTableName("user_behavior");
    List<ColumnStatement> sinkTableColumns = new ArrayList<>();
    sinkTableColumns.add(new ColumnStatement("uid", "String"));
    sinkTableColumns.add(new ColumnStatement("age", "Integer"));
    sinkTableColumns.add(new ColumnStatement("isForeigners", "Boolean"));
    sinkTableColumns.add(new ColumnStatement("action", "String"));
    sinkTableColumns.add(new ColumnStatement("timestamp", "Long"));
    sinkTable.setColumns(sinkTableColumns);

    List<TableStatement> tableStatements = new ArrayList<>();
    tableStatements.add(inputTable);
    tableStatements.add(sinkTable);

    System.out.println(JsonUtils.toJson(tableStatements));


  }

}

package com.sdu.flink.table.functions.template;

import com.sdu.flink.table.functions.enhance.TableFunctionEnhance;
import org.apache.flink.types.Row;

@TableFunctionEnhance
public class SimpleTableFunction extends QTableFunction<Row> {

  @Override
  public Row getDefaultValue() {
    return null;
  }


}

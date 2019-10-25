package org.apache.flink.types;


import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableMap;

public class SqlTypeToJavaTypeConverts {

  private static Map<SqlTypeName, Class<?>> SqlTypeToJavaTypes = ImmutableMap.<SqlTypeName, Class<?>>builder()
      .put(SqlTypeName.BOOLEAN, Boolean.class)
      // String
      .put(SqlTypeName.CHAR, Character.class)
      .put(SqlTypeName.VARCHAR, String.class)
      // Number
      .put(SqlTypeName.SMALLINT, Byte.class)
      .put(SqlTypeName.INTEGER, Integer.class)
      .put(SqlTypeName.BIGINT, Long.class)
      .put(SqlTypeName.REAL, BigDecimal.class)
      .put(SqlTypeName.FLOAT, Float.class)
      .put(SqlTypeName.DOUBLE, Double.class)
      // byte[]
      .put(SqlTypeName.VARBINARY, Byte[].class)
      .put(SqlTypeName.ARRAY, List.class)

      // date
      .put(SqlTypeName.DATE, Date.class)
      .put(SqlTypeName.TIMESTAMP, Timestamp.class)
      .put(SqlTypeName.TIME, Time.class)
      .put(SqlTypeName.DECIMAL, BigDecimal.class)

      .build();

  private SqlTypeToJavaTypeConverts() {
  }

  public static Class<?> sqlTypeToJavaType(SqlTypeName sqlTypeName) {
    Class<?> javaType = SqlTypeToJavaTypes.get(sqlTypeName);
    if (javaType == null) {
      throw new UnsupportedSqlJavaTypeException(sqlTypeName);
    }
    return javaType;
  }

  public static String sqlTypeToJavaTypeAsString(SqlTypeName sqlTypeName) {
    return sqlTypeToJavaType(sqlTypeName).getSimpleName();
  }

}

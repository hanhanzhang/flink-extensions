package org.apache.flink.types;


import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
import static org.apache.calcite.sql.type.SqlTypeName.CHAR;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.FLOAT;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.REAL;
import static org.apache.calcite.sql.type.SqlTypeName.SMALLINT;
import static org.apache.calcite.sql.type.SqlTypeName.TINYINT;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableMap;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.exec.DRexUnsupportedException;
import org.apache.flink.table.types.DataType;

/**
 * 支持类型:
 *
 * String, Integer, Long, Double, Float, Boolean, BigDecimal
 *
 * TODO: BigDecimal 精度
 *
 * @author hanhan.zhang
 * */
public class DSqlTypeUtils {

  private static final int DEFAULT_SCALE = 8;

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

      .build();


  private static Map<String, SqlTypeName> javaTypeToSqlTypes = ImmutableMap.<String, SqlTypeName>builder()
      // Boolean
      .put(Boolean.class.getSimpleName(), SqlTypeName.BOOLEAN)

      // String
      .put(Character.class.getSimpleName(), SqlTypeName.CHAR)
      .put(String.class.getSimpleName(), SqlTypeName.VARCHAR)

      // Number
      .put(Byte.class.getSimpleName(), SqlTypeName.SMALLINT)
      .put(Integer.class.getSimpleName(), SqlTypeName.INTEGER)
      .put(Long.class.getSimpleName(), SqlTypeName.BIGINT)
      .put(BigDecimal.class.getSimpleName(), SqlTypeName.REAL)
      .put(Float.class.getSimpleName(), SqlTypeName.FLOAT)
      .put(Double.class.getSimpleName(), SqlTypeName.DOUBLE)

      .build();

  private DSqlTypeUtils() {
  }

  public static Class<?> sqlTypeToJavaType(SqlTypeName sqlTypeName) {
    Class<?> javaType = SqlTypeToJavaTypes.get(sqlTypeName);
    if (javaType == null) {
      throw new UnsupportedTypeException(sqlTypeName.getName());
    }
    return javaType;
  }

  public static String sqlTypeToJavaTypeAsString(SqlTypeName sqlTypeName) {
    return sqlTypeToJavaType(sqlTypeName).getSimpleName();
  }

  public static DataType javaTypeToDataType(String javaType) {
    switch (javaType) {
      case "Boolean":
        return DataTypes.BOOLEAN();
      case "BigDecimal":
        return DataTypes.DECIMAL(10, 0);
      case "Integer":
        return DataTypes.INT();
      case "Long":
        return DataTypes.BIGINT();
      case "Double":
        return DataTypes.DOUBLE();
      case "Float":
        return DataTypes.FLOAT();
      case "String":
        return DataTypes.STRING();
      default:
        throw new UnsupportedTypeException(javaType);
    }
  }

  public static boolean isNumeric(SqlTypeName sqlTypeName) {
    return sqlTypeName == INTEGER || sqlTypeName == TINYINT || sqlTypeName == SMALLINT || sqlTypeName == BIGINT
        || sqlTypeName == FLOAT || sqlTypeName == DOUBLE;
  }

  public static boolean isBigDecimal(SqlTypeName sqlTypeName) {
    return sqlTypeName == REAL;
  }

  public static boolean isString(SqlTypeName sqlTypeName) {
    return sqlTypeName == VARCHAR || sqlTypeName == CHAR;
  }

  public static boolean isBoolean(SqlTypeName sqlTypeName) {
    return sqlTypeName == BOOLEAN;
  }

  public static boolean canCastToDouble(SqlTypeName sqlTypeName) {
    return sqlTypeName == FLOAT || sqlTypeName == DOUBLE || sqlTypeName == REAL;
  }

  // --------------------------------------------------------------------------------------
  // String convert Number, Boolean, Character
  // --------------------------------------------------------------------------------------
  public static Number stringToNumber(String fromValue, SqlTypeName target) {
    switch (target) {
      case INTEGER:
        return Integer.valueOf(fromValue);
      case BIGINT:
        return Long.valueOf(fromValue);
      case SMALLINT:
        return Byte.valueOf(fromValue);
      case FLOAT:
        return Float.valueOf(fromValue);
      case DOUBLE:
        return Double.valueOf(fromValue);
      case REAL:
        return new BigDecimal(fromValue);

      default:
        throw new UnsupportedOperationException("Unsupported number type: " + target);
    }
  }

  public static Boolean stringToBoolean(String fromValue) {
    return Boolean.valueOf(fromValue);
  }

  private static Character stringToChar(String fromValue) {
    char[] chars = fromValue.toCharArray();
    assert chars.length == 1;
    return chars[0];
  }

  // --------------------------------------------------------------------------------------
  // Number convert Number, String, Boolean, Character
  // --------------------------------------------------------------------------------------
  public static Number numberToNumber(Number numberValue, SqlTypeName from, SqlTypeName to) {
    if (from == to) {
      return numberValue;
    }

    switch (from) {
      case INTEGER:
      {
        switch (to) {
          case SMALLINT:
            return numberValue.byteValue();
          case DOUBLE:
            return numberValue.doubleValue();
          case FLOAT:
            return numberValue.floatValue();
          case BIGINT:
            return numberValue.longValue();
          case REAL:
            return new BigDecimal(numberValue.intValue());
          default:
            throw new DRexUnsupportedException("Unsupported convert '" + from + "' to '" + to + "'.");
        }
      }
      case DOUBLE:
      {
        switch (to) {
          case SMALLINT:
            return numberValue.byteValue();
          case INTEGER:
            return numberValue.intValue();
          case FLOAT:
            return numberValue.floatValue();
          case BIGINT:
            return numberValue.longValue();
          case REAL:
            return new BigDecimal(numberValue.doubleValue());
          default:
            throw new DRexUnsupportedException("Unsupported convert '" + from + "' to '" + to + "'.");
        }
      }
      case FLOAT:
      {
        switch (to) {
          case SMALLINT:
            return numberValue.byteValue();
          case INTEGER:
            return numberValue.intValue();
          case DOUBLE:
            return numberValue.doubleValue();
          case BIGINT:
            return numberValue.longValue();
          case REAL:
            return new BigDecimal(numberValue.floatValue());
          default:
            throw new DRexUnsupportedException("Unsupported convert '" + from + "' to '" + to + "'.");
        }
      }
      case BIGINT:
      {
        {
          switch (to) {
            case SMALLINT:
              return numberValue.byteValue();
            case INTEGER:
              return numberValue.intValue();
            case DOUBLE:
              return numberValue.doubleValue();
            case FLOAT:
              return numberValue.floatValue();
            case REAL:
              return new BigDecimal(numberValue.longValue());
            default:
              throw new DRexUnsupportedException("Unsupported convert '" + from + "' to '" + to + "'.");
          }
        }
      }
      case REAL:
      {
        {
          switch (to) {
            case SMALLINT:
              return numberValue.byteValue();
            case INTEGER:
              return numberValue.intValue();
            case DOUBLE:
              return numberValue.doubleValue();
            case FLOAT:
              return numberValue.floatValue();
            case BIGINT:
              return numberValue.longValue();
            default:
              throw new DRexUnsupportedException("Unsupported convert '" + from + "' to '" + to + "'.");
          }
        }
      }
      default:
        throw new DRexUnsupportedException("Unsupported number type: '" + from +"'.");
    }
  }

  public static String numberToString(Object numberValue) {
    if (numberValue instanceof BigDecimal) {
      BigDecimal value = (BigDecimal) numberValue;
      value = value.setScale(DEFAULT_SCALE, RoundingMode.HALF_UP);
      return value.toPlainString();
    }

    return String.valueOf(numberValue);
  }

  public static Boolean numberToBoolean(Number numberValue) {
    String className = numberValue.getClass().getSimpleName();
    switch (className) {
      case "Integer":
        return numberValue.intValue() == 1;
      case "Long":
        return numberValue.longValue() == 1;
      case "Byte":
        return numberValue.byteValue() == 1;
      case "Float":
        return numberValue.floatValue() == 0.0f;
      case "Double":
        return numberValue.doubleValue() == 0.0d;
      case "BigDecimal":
        BigDecimal decimalValue = (BigDecimal) numberValue;
        return decimalValue.equals(BigDecimal.ONE);
      default:
        throw new DRexUnsupportedException("unsupported number type: " + className);
    }
  }

  private static Character numberToCharacter(Number numberValue, SqlTypeName sqlType) {
    throw new DRexUnsupportedException("Unsupported convert from '" + sqlType + "' to 'CHAR'.");
  }

  // --------------------------------------------------------------------------------------
  // Boolean convert Number, String, Character
  // --------------------------------------------------------------------------------------
  public static Number booleanToNumber(boolean value, SqlTypeName target) {
    switch (target) {
      case INTEGER:
        return value ? 1 : 0;
      case SMALLINT:
        return value ? (byte) 1 : (byte) 0;
      case BIGINT:
        return value ? 1L : 0L;
      case FLOAT:
        return value ? 1.0f : 0.0f;
      case DOUBLE:
        return value ? 1.0d : 0.0f;
      case REAL:
        return value ? BigDecimal.ONE : BigDecimal.ZERO;
      default:
        throw new DRexUnsupportedException("Unsupported number type: " + target);

    }
  }

  private static Character booleanToCharacter(boolean value) {
    return value ? '1' : '0';
  }

  // --------------------------------------------------------------------------------------
  // Character convert Number, String, Boolean
  // --------------------------------------------------------------------------------------
  private static Number characterToNumber(Character c, SqlTypeName sqlTypeName) {
    throw new DRexUnsupportedException("Unsupported convert from 'CHAR' to '" + sqlTypeName + "'.");
  }

  private static String characterToString(Character c) {
    return String.valueOf(c);
  }

  private static Boolean characterToBoolean(Character c) {
    return c != '0';
  }

  public static String booleanToString(boolean value) {
    return String.valueOf(value);
  }

  public static Object stringToObject(String objectValue, SqlTypeName target) {
    switch (target) {
      case INTEGER:
      case BIGINT:
      case SMALLINT:
      case FLOAT:
      case DOUBLE:
        return stringToNumber(objectValue, target);
      case REAL:
        return new BigDecimal(objectValue);

        case BOOLEAN:
        return Boolean.valueOf(objectValue);

        case VARCHAR:
        return objectValue;
      case CHAR:
        char[] chars = objectValue.toCharArray();
        assert chars.length == 1;
        return chars[0];

      default:
        throw new UnsupportedOperationException("Unsupported SqlType: " + target);
    }
  }

  public static String objectToString(Object objectValue, SqlTypeName resultType) {
    switch (resultType) {
      case INTEGER:
      case BIGINT:
      case SMALLINT:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case CHAR:
        return String.valueOf(objectValue);

      case VARCHAR:
        return (String) objectValue;

      case REAL:
        BigDecimal value = (BigDecimal) objectValue;
        value = value.setScale(DEFAULT_SCALE, RoundingMode.HALF_UP);
        return value.toPlainString();

      default:
        throw new UnsupportedOperationException("Unsupported SqlType: " + resultType);
    }
  }

  public static Object castObject(Object objectValue, SqlTypeName resultType) {
    String className = objectValue.getClass().getSimpleName();
    switch (className) {
      case "Integer":
      {
        Integer value = (Integer) objectValue;
        switch (resultType) {
          case INTEGER:
          case BIGINT:
          case SMALLINT:
          case FLOAT:
          case DOUBLE:
          case REAL:
            return numberToNumber(value, INTEGER, resultType);

          case BOOLEAN:
            return numberToBoolean(value);

          case CHAR:
            return numberToCharacter(value, INTEGER);
          case VARCHAR:
            return numberToString(value);

          default:
            throw new UnsupportedOperationException("Unsupported SqlType: " + resultType);
        }
      }
      case "Long":
      {
        Long value = (Long) objectValue;
        switch (resultType) {
          case INTEGER:
          case BIGINT:
          case SMALLINT:
          case FLOAT:
          case DOUBLE:
          case REAL:
            return numberToNumber(value, BIGINT, resultType);

          case BOOLEAN:
            return numberToBoolean(value);

          case CHAR:
            return numberToCharacter(value, BIGINT);
          case VARCHAR:
            return numberToString(value);

          default:
            throw new UnsupportedOperationException("Unsupported SqlType: " + resultType);
        }
      }
      case "Float":
      {
        Float value = (Float) objectValue;
        switch (resultType) {
          case INTEGER:
          case BIGINT:
          case SMALLINT:
          case FLOAT:
          case DOUBLE:
          case REAL:
            return numberToNumber(value, FLOAT, resultType);

          case BOOLEAN:
            return numberToBoolean(value);

          case CHAR:
            return numberToCharacter(value, FLOAT);
          case VARCHAR:
            return numberToString(value);

          default:
            throw new UnsupportedOperationException("Unsupported SqlType: " + resultType);
        }
      }
      case "Double":
      {
        Double value = (Double) objectValue;
        switch (resultType) {
          case INTEGER:
          case BIGINT:
          case SMALLINT:
          case FLOAT:
          case DOUBLE:
          case REAL:
            return numberToNumber(value, DOUBLE, resultType);

          case BOOLEAN:
            return numberToBoolean(value);

          case CHAR:
            return numberToCharacter(value, DOUBLE);
          case VARCHAR:
            return numberToString(value);

          default:
            throw new UnsupportedOperationException("Unsupported SqlType: " + resultType);
        }
      }
      case "BigDecimal":
      {
        BigDecimal value = (BigDecimal) objectValue;
        switch (resultType) {
          case INTEGER:
          case BIGINT:
          case SMALLINT:
          case FLOAT:
          case DOUBLE:
          case REAL:
            return numberToNumber(value, REAL, resultType);

          case BOOLEAN:
            return numberToBoolean(value);

          case CHAR:
            return numberToCharacter(value, REAL);
          case VARCHAR:
            return numberToString(value);

          default:
            throw new UnsupportedOperationException("Unsupported SqlType: " + resultType);
        }
      }

      case "Boolean":
      {
        boolean value = (boolean) objectValue;
        switch (resultType) {
          case INTEGER:
          case BIGINT:
          case SMALLINT:
          case FLOAT:
          case DOUBLE:
          case REAL:
            return booleanToNumber(value, resultType);

          case BOOLEAN:
            return objectValue;

          case CHAR:
            return booleanToCharacter(value);
          case VARCHAR:
           return booleanToString(value);

          default:
            throw new UnsupportedOperationException("Unsupported SqlType: " + resultType);
        }
      }

      case "String":
      {
        String value = (String) objectValue;
        switch (resultType) {
          case INTEGER:
          case BIGINT:
          case SMALLINT:
          case FLOAT:
          case DOUBLE:
          case REAL:
            return stringToNumber(value, resultType);

          case BOOLEAN:
            return stringToBoolean(value);

          case CHAR:
            return stringToChar(value);
          case VARCHAR:
            return value;

          default:
            throw new UnsupportedOperationException("Unsupported SqlType: " + resultType);
        }
      }
      case "Character":
      {
        Character value = (Character) objectValue;
        switch (resultType) {
          case INTEGER:
          case BIGINT:
          case SMALLINT:
          case FLOAT:
          case DOUBLE:
          case REAL:
              return characterToNumber(value, resultType);

          case BOOLEAN:
            return characterToBoolean(value);

          case CHAR:
           return objectValue;
          case VARCHAR:
            return characterToString(value);

          default:
            throw new UnsupportedOperationException("Unsupported SqlType: " + resultType);
        }
      }

      default:
        throw new UnsupportedOperationException("Unsupported javaType: " + className);
    }
  }

  public static DataType fromTableSchema(TableSchema tableSchema) {
    DataTypes.Field[] fields = new DataTypes.Field[tableSchema.getFieldCount()];
    for (int i = 0; i < tableSchema.getFieldCount(); ++i) {
      fields[i] = DataTypes.FIELD(tableSchema.getFieldNames()[i],
          tableSchema.getFieldDataTypes()[i]);
    }
    return DataTypes.ROW(fields);
  }

}

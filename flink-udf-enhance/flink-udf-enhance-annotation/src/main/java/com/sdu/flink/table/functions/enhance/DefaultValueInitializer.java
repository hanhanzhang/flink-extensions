package com.sdu.flink.table.functions.enhance;

import java.util.Collections;
import java.util.Map;

public interface DefaultValueInitializer {

  default boolean getDefaultBooleanValue() {
    return false;
  }

  default double getDefaultDoubleValue() {
    return 0.0d;
  }

  default float getDefaultFloatValue() {
    return 0.0f;
  }

  default int getDefaultIntValue() {
    return 0;
  }

  default short getDefaultShortValue() {
    return 0;
  }

  default long getDefaultLongValue() {
    return 0L;
  }

  default byte getDefaultByteValue() {
    return 0;
  }

  default Object[] getDefaultArrayValue() {
    return null;
  }

  default String getDefaultStringValue() {
    return "";
  }

  default Map getDefaultMapValue() {
    return Collections.EMPTY_MAP;
  }
}
